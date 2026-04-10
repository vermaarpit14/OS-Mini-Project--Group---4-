/*
 * node.c — Unified P2P Grid Node
 * ─────────────────────────────────────────────────────────────────────────────
 * Every node is simultaneously a leader-candidate AND a worker.
 *
 * Discovery  : UDP multicast 239.0.0.1:9090  (BeaconMsg every 3s)
 * Election   : Bully algorithm — highest node_id (IP as uint32) wins
 * Job routing: Leader assigns to least-loaded peer; falls back to self
 * Failover   : Worker dies → leader seamlessly reassigns to another worker
 * Leader dies → election → new leader; workers report live jobs
 * ─────────────────────────────────────────────────────────────────────────────
 */
#include "common.h"

#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <inttypes.h>
#include <net/if.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* ── Tunables ────────────────────────────────────────────────────────────────
 */
#define MAX_PEERS 16
#define HB_INTERVAL 1    /* send heartbeat every N seconds          */
#define PEER_DEAD_SECS 5 /* N seconds without HB → peer dead        */
#define ELECTION_WAIT 3  /* wait N secs for higher-ID node to ack   */
#define MAX_LOG_LINES 10
#define BEACON_INTERVAL 3 /* UDP multicast every N seconds          */
#define MAX_STRIKES 3     /* strikes before ban                      */

/* ── Roles ───────────────────────────────────────────────────────────────────
 */
typedef enum { FOLLOWER = 0, CANDIDATE = 1, LEADER = 2 } Role;
static const char *ROLE_NAME[] = {"WORKER", "CANDIDATE", "APEX"};

/* ── Per-peer entry ──────────────────────────────────────────────────────────
 */
typedef struct {
  uint32_t id; /* peer's IP as network-order uint32     */
  char ip[16];
  int fd;                /* TCP fd to this peer (-1 = none)       */
  time_t last_hb;        /* last heartbeat received (monotonic)   */
  float cpu_avg;         /* last reported average CPU %           */
  int slot_used;         /* 0 = empty slot                        */
  pthread_mutex_t tx_mu; /* protects writes to fd                 */
} Peer;

/* ── Job record (held by leader) ─────────────────────────────────────────────
 */
typedef struct Job {
  uint64_t id;
  int disp_fd;        /* dispatcher socket fd                  */
  uint32_t worker_id; /* node_id of executor; 0 = unassigned   */
  uint8_t type;       /* MSG_EXEC_WORK / MSG_PROJECT_WORK      */
  char *payload;
  uint32_t payload_len;
  int active;
  struct Job *next;
} Job;

/* ── Dispatcher client record (leader only, for strikes / TUI) ───────────────
 */
typedef struct {
  int fd;
  int strikes;
  int is_banned;
  char ip[16];
  int used;
} DispClient;

/* ── Queued job (waiting for a free worker) ──────────────────────────────────
 */
typedef struct {
  int sender_fd;
  uint8_t *payload;
  uint32_t payload_len;
  int is_project;
} QEntry;

/* ══════════════════════════════════════════════════════════════════════════════
   Global state
   ══════════════════════════════════════════════════════════════════════════════
 */
static uint32_t g_my_id;
static char g_my_ip[16];

static Role g_role = FOLLOWER;
static uint32_t g_leader_id = 0;
static time_t g_last_ldr_hb = 0;

static Peer g_peers[MAX_PEERS];
static int g_npeers = 0;
static pthread_mutex_t g_peers_mu = PTHREAD_MUTEX_INITIALIZER;

static Job *g_jobs = NULL;
static uint64_t g_next_jid = 1;
static pthread_mutex_t g_jobs_mu = PTHREAD_MUTEX_INITIALIZER;

static DispClient g_disps[MAX_CLIENTS];
static pthread_mutex_t g_disp_mu = PTHREAD_MUTEX_INITIALIZER;

static QEntry g_queue[MAX_QUEUE_SIZE];
static int q_head = 0, q_tail = 0, q_size = 0;
static pthread_mutex_t g_queue_mu = PTHREAD_MUTEX_INITIALIZER;

/* Election state */
static volatile int g_elec_active = 0;
static volatile int g_elec_ok_seen = 0;
static pthread_mutex_t g_elec_mu = PTHREAD_MUTEX_INITIALIZER;

/* Worker-side: what this node is currently executing */
static volatile uint64_t g_my_job_id = 0;
static volatile int g_active_stdin_fd = -1;
static char *g_my_job_payload = NULL;
static uint32_t g_my_job_plen = 0;
static uint8_t g_my_job_type = 0;
static pthread_mutex_t g_worker_mu = PTHREAD_MUTEX_INITIALIZER;

/* Log lines for TUI */
static char g_log[MAX_LOG_LINES][512];
static int g_log_n = 0;
static pthread_mutex_t g_log_mu = PTHREAD_MUTEX_INITIALIZER;

static int g_listen_fd = -1;

/* Forward declarations */
static void become_leader(void);
static void start_election(void);
static void assign_job(Job *j);
static void reassign_jobs_from(uint32_t dead_worker_id);
static int peer_send_locked(Peer *p, MsgType t, const void *pl, uint32_t len);
static void check_and_dequeue(void);

/* ══════════════════════════════════════════════════════════════════════════════
   Logging & TUI
   ══════════════════════════════════════════════════════════════════════════════
 */
static void grid_log(const char *color, const char *fmt, ...) {
  char tmp[384], msg[512];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(tmp, sizeof tmp, fmt, ap);
  va_end(ap);
  time_t now = time(NULL);
  char ts[16];
  strftime(ts, sizeof ts, "%H:%M:%S", localtime(&now));
  /* Changed format to be distinctly different */
  snprintf(msg, sizeof msg, C_RESET "(%s) ~> %s%s" C_RESET, ts, color, tmp);

  pthread_mutex_lock(&g_log_mu);
  if (g_log_n < MAX_LOG_LINES)
    strcpy(g_log[g_log_n++], msg);
  else {
    for (int i = 1; i < MAX_LOG_LINES; i++)
      strcpy(g_log[i - 1], g_log[i]);
    strcpy(g_log[MAX_LOG_LINES - 1], msg);
  }
  pthread_mutex_unlock(&g_log_mu);
}

static void write_ledger(const char *event, const char *ip,
                         const char *detail) {
  FILE *f = fopen("grid_ledger.csv", "a");
  if (!f)
    return;
  time_t now = time(NULL);
  char ts[64];
  strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", localtime(&now));
  fseek(f, 0, SEEK_END);
  if (ftell(f) == 0)
    fprintf(f, "Timestamp,Event,IP,Detail\n");
  fprintf(f, "%s,%s,%s,\"%s\"\n", ts, event, ip, detail);
  fclose(f);
}

static void render_tui(void) {
  printf("\x1b[H\x1b[?25l");
  printf(C_BG_MAGENTA C_BOLD " === OS MINI PROJECT === " C_RESET "\n");
  printf(C_WHITE " [*] LOCAL BIND : %-15s \n"
                 " [*] NODE STATE : %-10s \n"
                 " [*] APEX NODE  : %-15s \n" C_RESET,
         g_my_ip, ROLE_NAME[(int)g_role],
         g_leader_id ? (g_role == LEADER ? g_my_ip : "...") : "NEGOTIATING");

  /* peers table */
  printf(C_GREEN "\n++[ ACTIVE TOPOLOGY ]+++++++++++++++++++++++++++++++++++++++\n"
                 "| " C_BOLD "%-16s" C_GREEN " | " C_BOLD "%-9s" C_GREEN
                 " | " C_BOLD "%-10s" C_GREEN " | " C_BOLD "%-9s" C_GREEN " |\n"
                 "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n",
         "ADDRESS", "HEALTH", "LOAD(%)", "APEX FLAG");

  int shown = 0;
  pthread_mutex_lock(&g_peers_mu);
  for (int i = 0; i < MAX_PEERS; i++) {
    if (!g_peers[i].slot_used)
      continue;
    shown++;
    const char *st =
        g_peers[i].fd >= 0 ? C_GREEN "ONLINE " C_GREEN : C_RED "OFFLINE" C_GREEN;
    printf("│ %-16s │ %-18s │ %7.1f%%   │ %-9s │\n", g_peers[i].ip, st,
           g_peers[i].cpu_avg,
           g_peers[i].id == g_leader_id ? C_YELLOW "TRUE " C_GREEN : "false");
  }
  pthread_mutex_unlock(&g_peers_mu);
  for (int i = shown; i < 3; i++)
    printf("│                  │           │            │           │\n");
  printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n" C_RESET);

  if (g_role == LEADER) {
    pthread_mutex_lock(&g_queue_mu);
    printf(C_YELLOW C_BOLD "\n ==[ PENDING BACKLOG: %d ]==\n" C_RESET, q_size);
    pthread_mutex_unlock(&g_queue_mu);
  }

  printf(C_BOLD "\n >> EVENT STREAM <<\n" C_RESET);
  pthread_mutex_lock(&g_log_mu);
  for (int i = 0; i < g_log_n; i++)
    printf(" %s\n", g_log[i]);
  pthread_mutex_unlock(&g_log_mu);
  printf("\x1b[J");
  fflush(stdout);
}

/* ══════════════════════════════════════════════════════════════════════════════
   Network identity
   ══════════════════════════════════════════════════════════════════════════════
 */
static uint32_t get_my_ip(char out[16]) {
  struct ifaddrs *ifa, *p;
  uint32_t found = 0;
  if (getifaddrs(&ifa) != 0) {
    strcpy(out, "127.0.0.1");
    return htonl(0x7f000001);
  }
  for (p = ifa; p; p = p->ifa_next) {
    if (!p->ifa_addr || p->ifa_addr->sa_family != AF_INET)
      continue;
    if (strcmp(p->ifa_name, "lo") == 0)
      continue;
    struct sockaddr_in *s = (struct sockaddr_in *)p->ifa_addr;
    inet_ntop(AF_INET, &s->sin_addr, out, 16);
    found = s->sin_addr.s_addr;
    break;
  }
  freeifaddrs(ifa);
  if (!found) {
    strcpy(out, "127.0.0.1");
    found = htonl(0x7f000001);
  }
  return found;
}

/* ══════════════════════════════════════════════════════════════════════════════
   Peer management
   ══════════════════════════════════════════════════════════════════════════════
 */
/* Returns peer index or -1 if not found */
static int find_peer_idx(uint32_t id) {
  for (int i = 0; i < MAX_PEERS; i++)
    if (g_peers[i].slot_used && g_peers[i].id == id)
      return i;
  return -1;
}

/* Adds or updates a peer. Returns index, -1 on table full / duplicate fd */
static int upsert_peer(uint32_t id, const char *ip, int fd) {
  pthread_mutex_lock(&g_peers_mu);
  /* Already have this peer? Update fd if better */
  int idx = find_peer_idx(id);
  if (idx >= 0) {
    if (fd >= 0 && g_peers[idx].fd < 0) {
      g_peers[idx].fd = fd;
      g_peers[idx].last_hb = time(NULL);
    } else if (fd >= 0 && g_peers[idx].fd != fd) {
      /* duplicate connection: close new one, keep old */
      pthread_mutex_unlock(&g_peers_mu);
      close(fd);
      return idx;
    }
    pthread_mutex_unlock(&g_peers_mu);
    return idx;
  }
  /* New peer */
  for (int i = 0; i < MAX_PEERS; i++) {
    if (!g_peers[i].slot_used) {
      g_peers[i].id = id;
      g_peers[i].fd = fd;
      g_peers[i].last_hb = time(NULL);
      g_peers[i].cpu_avg = 0.0f;
      g_peers[i].slot_used = 1;
      strncpy(g_peers[i].ip, ip, 15);
      pthread_mutex_init(&g_peers[i].tx_mu, NULL);
      g_npeers++;
      pthread_mutex_unlock(&g_peers_mu);
      return i;
    }
  }
  pthread_mutex_unlock(&g_peers_mu);
  return -1;
}

static void kill_peer(int idx) {
  if (idx < 0 || !g_peers[idx].slot_used)
    return;
  if (g_peers[idx].fd >= 0) {
    close(g_peers[idx].fd);
    g_peers[idx].fd = -1;
  }
}

/* Thread-safe send to a peer by index */
static int peer_send_locked(Peer *p, MsgType t, const void *pl, uint32_t len) {
  if (!p || p->fd < 0)
    return -1;
  pthread_mutex_lock(&p->tx_mu);
  int r = send_p2p(p->fd, t, pl, len);
  pthread_mutex_unlock(&p->tx_mu);
  return r;
}

/* Broadcast a P2P message to all live peers */
static void broadcast_peers(MsgType t, const void *pl, uint32_t len) {
  pthread_mutex_lock(&g_peers_mu);
  for (int i = 0; i < MAX_PEERS; i++) {
    if (g_peers[i].slot_used && g_peers[i].fd >= 0)
      peer_send_locked(&g_peers[i], t, pl, len);
  }
  pthread_mutex_unlock(&g_peers_mu);
}

/* ══════════════════════════════════════════════════════════════════════════════
   Job management (leader-side)
   ══════════════════════════════════════════════════════════════════════════════
 */
static Job *find_job(uint64_t id) {
  for (Job *j = g_jobs; j; j = j->next)
    if (j->id == id && j->active)
      return j;
  return NULL;
}

static Job *new_job(int disp_fd, uint8_t type, const char *payload,
                    uint32_t plen) {
  Job *j = calloc(1, sizeof(Job));
  j->id = g_next_jid++;
  j->disp_fd = disp_fd;
  j->worker_id = 0;
  j->type = type;
  j->payload_len = plen;
  j->payload = malloc(plen);
  memcpy(j->payload, payload, plen);
  j->active = 1;
  j->next = g_jobs;
  g_jobs = j;
  return j;
}

static void finish_job(uint64_t id) {
  pthread_mutex_lock(&g_jobs_mu);
  for (Job *j = g_jobs; j; j = j->next) {
    if (j->id == id && j->active) {
      j->active = 0;
      if (j->payload) {
        free(j->payload);
        j->payload = NULL;
      }
      break;
    }
  }
  pthread_mutex_unlock(&g_jobs_mu);
}

/* ── Worker finder for load-balancing ─────────────────────────────────────────
 */
static int find_best_worker_idx(void) {
  /* Must be called with g_peers_mu held */
  int best = -1;
  float low = 200.0f;
  for (int i = 0; i < MAX_PEERS; i++) {
    if (!g_peers[i].slot_used || g_peers[i].fd < 0)
      continue;
    if (g_peers[i].cpu_avg < low) {
      low = g_peers[i].cpu_avg;
      best = i;
    }
  }
  return best;
}

/* Send MSG_JOB_ASSIGN to a peer for a given job */
static void send_job_to_peer(int peer_idx, Job *j) {
  /* Build: JobAssignMsg header + payload */
  size_t total = sizeof(JobAssignMsg) + j->payload_len;
  char *buf = malloc(total);
  JobAssignMsg *hdr = (JobAssignMsg *)buf;
  hdr->job_id = j->id;
  hdr->job_type = j->type;
  hdr->code_len = j->payload_len;
  memcpy(buf + sizeof(JobAssignMsg), j->payload, j->payload_len);
  peer_send_locked(&g_peers[peer_idx], MSG_JOB_ASSIGN, buf, (uint32_t)total);
  free(buf);
}

/* ══════════════════════════════════════════════════════════════════════════════
   Security scanner (same heuristic as original server.c)
   ══════════════════════════════════════════════════════════════════════════════
 */
static int scan_for_malware(const char *code) {
  return strstr(code, "system(") != NULL || strstr(code, "execvp") != NULL ||
         strstr(code, "execve") != NULL || strstr(code, "remove(") != NULL ||
         strstr(code, "unlink(") != NULL;
}

/* ── Dispatcher strike tracker ────────────────────────────────────────────────
 */
static DispClient *find_disp(int fd) {
  for (int i = 0; i < MAX_CLIENTS; i++)
    if (g_disps[i].used && g_disps[i].fd == fd)
      return &g_disps[i];
  return NULL;
}
static DispClient *add_disp(int fd, const char *ip) {
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (!g_disps[i].used) {
      g_disps[i].fd = fd;
      g_disps[i].strikes = 0;
      g_disps[i].is_banned = 0;
      g_disps[i].used = 1;
      strncpy(g_disps[i].ip, ip, 15);
      return &g_disps[i];
    }
  }
  return NULL;
}
static void remove_disp(int fd) {
  for (int i = 0; i < MAX_CLIENTS; i++)
    if (g_disps[i].used && g_disps[i].fd == fd)
      g_disps[i].used = 0;
}

/* ══════════════════════════════════════════════════════════════════════════════
   Job queue (when all workers busy)
   ══════════════════════════════════════════════════════════════════════════════
 */
static void enqueue_job(int sender_fd, uint8_t *payload, uint32_t len,
                        int is_project) {
  if (q_size >= MAX_QUEUE_SIZE) {
    send_msg(sender_fd, MSG_REJECTED, "Cluster backlog full", 20);
    return;
  }
  g_queue[q_tail].sender_fd = sender_fd;
  g_queue[q_tail].payload_len = len;
  g_queue[q_tail].is_project = is_project;
  g_queue[q_tail].payload = malloc(len);
  memcpy(g_queue[q_tail].payload, payload, len);
  q_tail = (q_tail + 1) % MAX_QUEUE_SIZE;
  q_size++;
  grid_log(C_YELLOW, "<BACKLOG> Capacity reached, task enqueued. Size: %d",
           q_size);
  const char *msg = "\n[CODE]: Cluster saturated. Task entering backlog...\n";
  send_msg(sender_fd, MSG_STREAM_OUT, msg, strlen(msg));
}

static void check_and_dequeue(void) {
  /* Call with g_jobs_mu already held or standalone - check peers */
  pthread_mutex_lock(&g_queue_mu);
  if (q_size <= 0) {
    pthread_mutex_unlock(&g_queue_mu);
    return;
  }
  QEntry e = g_queue[q_head];
  q_head = (q_head + 1) % MAX_QUEUE_SIZE;
  q_size--;
  pthread_mutex_unlock(&g_queue_mu);

  pthread_mutex_lock(&g_jobs_mu);
  Job *j = new_job(e.sender_fd, e.is_project ? MSG_PROJECT_WORK : MSG_EXEC_WORK,
                   (char *)e.payload, e.payload_len);
  assign_job(j);
  pthread_mutex_unlock(&g_jobs_mu);
  free(e.payload);

  const char *msg = "\n[CODE]: Node provisioned. Execution initiated...\n";
  send_msg(e.sender_fd, MSG_STREAM_OUT, msg, strlen(msg));
}

/* ══════════════════════════════════════════════════════════════════════════════
   Job execution — worker side
   ══════════════════════════════════════════════════════════════════════════════
 */
typedef struct {
  uint64_t job_id;
  int pipe_out; /* read end of child stdout pipe */
  pid_t pid;
  int result_fd; /* fd to send tagged output to */
  int is_local;  /* 1 = send directly to disp_fd (leader local) */
  int disp_fd;   /* used only when is_local = 1 */
  char cleanup1[256];
  char cleanup2[256];
} JobMonCtx;

static void *job_monitor_thread(void *arg) {
  JobMonCtx *ctx = (JobMonCtx *)arg;
  char buf[4096];
  ssize_t n;

  if (ctx->is_local) {
    /* leader running locally: stream directly to dispatcher */
    while ((n = read(ctx->pipe_out, buf, sizeof buf)) > 0)
      send_msg(ctx->disp_fd, MSG_STREAM_OUT, buf, (uint32_t)n);
  } else {
    /* remote worker: wrap output in TaggedHdr, send to leader */
    size_t tagged_buf_sz = sizeof(TaggedHdr) + sizeof buf;
    char *tbuf = malloc(tagged_buf_sz);
    TaggedHdr *th = (TaggedHdr *)tbuf;
    th->job_id = ctx->job_id;
    while ((n = read(ctx->pipe_out, tbuf + sizeof(TaggedHdr), sizeof buf)) >
           0) {
      pthread_mutex_lock(&g_peers_mu);
      int idx = find_peer_idx(g_leader_id);
      if (idx >= 0 && g_peers[idx].fd >= 0)
        peer_send_locked(&g_peers[idx], MSG_TAGGED_OUT, tbuf,
                         (uint32_t)(sizeof(TaggedHdr) + n));
      pthread_mutex_unlock(&g_peers_mu);
    }
    free(tbuf);
  }

  int status;
  waitpid(ctx->pid, &status, 0);
  close(ctx->pipe_out);

  pthread_mutex_lock(&g_worker_mu);
  g_active_stdin_fd = -1;
  g_my_job_id = 0;
  pthread_mutex_unlock(&g_worker_mu);

  if (WIFSIGNALED(status)) {
    int sig = WTERMSIG(status);
    char err[160];
    if (sig == SIGKILL || sig == SIGXCPU || sig == SIGALRM)
      snprintf(err, sizeof err, "\n[CODE Fault]: Process killed (Timeout - 2s)\n");
    else if (sig == SIGSEGV)
      snprintf(err, sizeof err, "\n[CODE Fault]: SegFault (Memory Limits Exceeded)\n");
    else
      snprintf(err, sizeof err, "\n[CODE Fault]: Terminated by system signal %d\n",
               sig);

    if (ctx->is_local) {
      send_msg(ctx->disp_fd, MSG_EXEC_RESULT, err, strlen(err));
    } else {
      size_t sz = sizeof(TaggedHdr) + strlen(err);
      char *tb = malloc(sz);
      ((TaggedHdr *)tb)->job_id = ctx->job_id;
      memcpy(tb + sizeof(TaggedHdr), err, strlen(err));
      pthread_mutex_lock(&g_peers_mu);
      int idx = find_peer_idx(g_leader_id);
      if (idx >= 0 && g_peers[idx].fd >= 0)
        peer_send_locked(&g_peers[idx], MSG_TAGGED_ERR, tb, (uint32_t)sz);
      pthread_mutex_unlock(&g_peers_mu);
      free(tb);
    }
  } else {
    if (ctx->is_local) {
      send_msg(ctx->disp_fd, MSG_JOB_DONE, NULL, 0);
    } else {
      TaggedHdr th = {ctx->job_id};
      pthread_mutex_lock(&g_peers_mu);
      int idx = find_peer_idx(g_leader_id);
      if (idx >= 0 && g_peers[idx].fd >= 0)
        peer_send_locked(&g_peers[idx], MSG_TAGGED_DONE, &th, sizeof th);
      pthread_mutex_unlock(&g_peers_mu);
    }
  }

  /* cleanup temp files */
  if (strlen(ctx->cleanup1) > 0) {
    char cmd[320];
    snprintf(cmd, sizeof cmd, "rm -rf %s", ctx->cleanup1);
    system(cmd);
  }
  if (strlen(ctx->cleanup2) > 0)
    unlink(ctx->cleanup2);
  free(ctx);
  return NULL;
}

/*
 * Compile + fork-exec a job.
 * is_local=1 → stream directly to disp_fd (leader is running it locally)
 * is_local=0 → stream tagged output to leader peer
 */
static void execute_job(uint64_t job_id, uint8_t type, const char *payload,
                        uint32_t plen, int is_local, int disp_fd) {
  char bin[256] = {0}, c1[256] = {0}, c2[256] = {0};
  char compile_out[MAX_RESULT_TEXT] = {0};
  int compile_err = 0;

  if (type == MSG_EXEC_WORK) {
    char src[256];
    snprintf(src, sizeof src, "/tmp/node_%d_%llu.c", getpid(),
             (unsigned long long)job_id);
    snprintf(bin, sizeof bin, "/tmp/node_%d_%llu.out", getpid(),
             (unsigned long long)job_id);
    FILE *f = fopen(src, "w");
    if (f) {
      fwrite(payload, 1, plen, f);
      fclose(f);
    }
    char cmd[600];
    snprintf(cmd, sizeof cmd, "gcc -O2 %s -o %s 2>&1", src, bin);
    FILE *fp = popen(cmd, "r");
    if (fp) {
      fread(compile_out, 1, sizeof compile_out - 1, fp);
      if (pclose(fp))
        compile_err = 1;
    }
    strcpy(c1, src);
    strcpy(c2, bin);

  } else { /* MSG_PROJECT_WORK */
    char tar[256], dir[256];
    snprintf(tar, sizeof tar, "/tmp/node_%d_%llu.tar.gz", getpid(),
             (unsigned long long)job_id);
    snprintf(dir, sizeof dir, "/tmp/node_%d_%llu_dir", getpid(),
             (unsigned long long)job_id);
    FILE *f = fopen(tar, "wb");
    if (f) {
      fwrite(payload, 1, plen, f);
      fclose(f);
    }
    char ec[600], cc[600];
    snprintf(ec, sizeof ec, "mkdir -p %s && tar -xzf %s -C %s", dir, tar, dir);
    system(ec);
    snprintf(cc, sizeof cc, "cd %s && gcc -O2 *.c -o run.out 2>&1", dir);
    FILE *fp = popen(cc, "r");
    if (fp) {
      fread(compile_out, 1, sizeof compile_out - 1, fp);
      if (pclose(fp))
        compile_err = 1;
    }
    snprintf(bin, sizeof bin, "%s/run.out", dir);
    strcpy(c1, dir);
    strcpy(c2, tar);
  }

  if (compile_err) {
    if (is_local) {
      send_msg(disp_fd, MSG_EXEC_RESULT, compile_out, strlen(compile_out));
    } else {
      size_t sz = sizeof(TaggedHdr) + strlen(compile_out);
      char *tb = malloc(sz);
      ((TaggedHdr *)tb)->job_id = job_id;
      memcpy(tb + sizeof(TaggedHdr), compile_out, strlen(compile_out));
      pthread_mutex_lock(&g_peers_mu);
      int idx = find_peer_idx(g_leader_id);
      if (idx >= 0 && g_peers[idx].fd >= 0)
        peer_send_locked(&g_peers[idx], MSG_TAGGED_ERR, tb, (uint32_t)sz);
      pthread_mutex_unlock(&g_peers_mu);
      free(tb);
    }
    /* cleanup */
    if (strlen(c1) > 0) {
      char cmd[320];
      snprintf(cmd, sizeof cmd, "rm -rf %s", c1);
      system(cmd);
    }
    if (strlen(c2) > 0)
      unlink(c2);
    return;
  }

  /* fork + exec */
  int p_in[2], p_out[2];
  if (pipe(p_in) || pipe(p_out))
    return;
  pid_t pid = fork();

  if (pid == 0) {
    alarm(2);
    struct rlimit rl;
    rl.rlim_cur = 2;
    rl.rlim_max = 3;
    setrlimit(RLIMIT_CPU, &rl);
    rl.rlim_cur = rl.rlim_max = 256 * 1024 * 1024;
    setrlimit(RLIMIT_AS, &rl);
    rl.rlim_cur = rl.rlim_max = 8 * 1024 * 1024;
    setrlimit(RLIMIT_STACK, &rl);
    dup2(p_in[0], STDIN_FILENO);
    dup2(p_out[1], STDOUT_FILENO);
    dup2(p_out[1], STDERR_FILENO);
    close(p_in[1]);
    close(p_out[0]);
    execlp("stdbuf", "stdbuf", "-o0", "-e0", bin, (char *)NULL);
    execl(bin, bin, (char *)NULL);
    exit(1);
  }

  close(p_in[0]);
  close(p_out[1]);

  pthread_mutex_lock(&g_worker_mu);
  g_active_stdin_fd = p_in[1];
  g_my_job_id = job_id;
  pthread_mutex_unlock(&g_worker_mu);

  JobMonCtx *ctx = calloc(1, sizeof(JobMonCtx));
  ctx->job_id = job_id;
  ctx->pipe_out = p_out[0];
  ctx->pid = pid;
  ctx->is_local = is_local;
  ctx->disp_fd = disp_fd;
  strncpy(ctx->cleanup1, c1, sizeof ctx->cleanup1 - 1);
  strncpy(ctx->cleanup2, c2, sizeof ctx->cleanup2 - 1);

  pthread_t tid;
  pthread_create(&tid, NULL, job_monitor_thread, ctx);
  pthread_detach(tid);
}

/* ══════════════════════════════════════════════════════════════════════════════
   Job assignment (leader decides where to run)
   ══════════════════════════════════════════════════════════════════════════════
 */
static void assign_job(Job *j) {
  /* Caller must hold g_jobs_mu */
  pthread_mutex_lock(&g_peers_mu);
  int w = find_best_worker_idx();
  pthread_mutex_unlock(&g_peers_mu);

  if (w >= 0) {
    j->worker_id = g_peers[w].id;
    grid_log(C_MAGENTA, "<SCHEDULER> Task %llu mapped -> node %s",
             (unsigned long long)j->id, g_peers[w].ip);
    send_job_to_peer(w, j);
  } else {
    /* No peers: run locally */
    j->worker_id = g_my_id;
    grid_log(C_CYAN, "<SCHEDULER> Task %llu mapped -> loopback (local)",
             (unsigned long long)j->id);
    execute_job(j->id, j->type, j->payload, j->payload_len, /*is_local=*/1,
                j->disp_fd);
  }
}

static void reassign_jobs_from(uint32_t dead_worker_id) {
  pthread_mutex_lock(&g_jobs_mu);
  for (Job *j = g_jobs; j; j = j->next) {
    if (!j->active || j->worker_id != dead_worker_id)
      continue;
    grid_log(C_YELLOW,
             "<RECOVERY> Task %llu lost in transit. Forcing remap...",
             (unsigned long long)j->id);
    const char *msg =
        "\n[CODE]: Remote host unresponsive! Instigating seamless migration...\n";
    send_msg(j->disp_fd, MSG_STREAM_OUT, msg, strlen(msg));
    write_ledger("FAILOVER", "", "Job migrated after worker death");
    j->worker_id = 0;
    assign_job(j);
  }
  pthread_mutex_unlock(&g_jobs_mu);
}

/* ══════════════════════════════════════════════════════════════════════════════
   Leader election  — Bully algorithm
   ══════════════════════════════════════════════════════════════════════════════
 */
static void *election_timer_thread(void *arg) {
  (void)arg;
  sleep(ELECTION_WAIT);
  pthread_mutex_lock(&g_elec_mu);
  if (g_elec_active && !g_elec_ok_seen) {
    pthread_mutex_unlock(&g_elec_mu);
    become_leader();
  } else {
    g_elec_active = 0;
    pthread_mutex_unlock(&g_elec_mu);
  }
  return NULL;
}

static void start_election(void) {
  pthread_mutex_lock(&g_elec_mu);
  if (g_elec_active) {
    pthread_mutex_unlock(&g_elec_mu);
    return;
  }
  g_elec_active = 1;
  g_elec_ok_seen = 0;
  g_role = CANDIDATE;
  pthread_mutex_unlock(&g_elec_mu);

  grid_log(C_YELLOW, "<CONSENSUS> Initiating protocol. Local identifier: %u", g_my_id);

  NodeInfoMsg me = {g_my_id, ""};
  strncpy(me.ip, g_my_ip, 15);

  int sent = 0;
  pthread_mutex_lock(&g_peers_mu);
  for (int i = 0; i < MAX_PEERS; i++) {
    if (!g_peers[i].slot_used || g_peers[i].fd < 0)
      continue;
    if (g_peers[i].id > g_my_id) {
      peer_send_locked(&g_peers[i], MSG_ELECTION, &me, sizeof me);
      sent++;
    }
  }
  pthread_mutex_unlock(&g_peers_mu);

  if (sent == 0) {
    /* Nobody higher — I win immediately */
    become_leader();
  } else {
    pthread_t tid;
    pthread_create(&tid, NULL, election_timer_thread, NULL);
    pthread_detach(tid);
  }
}

static void become_leader(void) {
  pthread_mutex_lock(&g_elec_mu);
  g_elec_active = 0;
  pthread_mutex_unlock(&g_elec_mu);

  g_role = LEADER;
  g_leader_id = g_my_id;

  grid_log(C_GREEN C_BOLD, "<CONSENSUS> <<< APEX ROLE ASSUMED >>> (%s)",
           g_my_ip);
  write_ledger("LEADER_ELECTED", g_my_ip, "This node won the election");

  NodeInfoMsg me = {g_my_id, ""};
  strncpy(me.ip, g_my_ip, 15);
  broadcast_peers(MSG_LEADER_ANN, &me, sizeof me);

  /* Re-assign any jobs that have no worker (orphaned by old leader's queue) */
  check_and_dequeue();
}

/* ══════════════════════════════════════════════════════════════════════════════
   Peer message loop  (one goroutine per peer TCP connection)
   ══════════════════════════════════════════════════════════════════════════════
 */
typedef struct {
  int peer_idx;
} PeerLoopArg;

static void *peer_loop_thread(void *arg) {
  int pidx = ((PeerLoopArg *)arg)->peer_idx;
  free(arg);

  Peer *p = &g_peers[pidx];
  grid_log(C_BLUE, "<LINK> Handshake successful: %s", p->ip);

  while (1) {
    MsgHeader hdr;
    if (recv_hdr(p->fd, &hdr) < 0)
      break;

    char *pl = NULL;
    if (hdr.payload_len > 0) {
      pl = malloc(hdr.payload_len + 1);
      if (recv_all(p->fd, pl, hdr.payload_len) < 0) {
        free(pl);
        break;
      }
      pl[hdr.payload_len] = '\0';
    }

    switch ((MsgType)hdr.type) {

    case MSG_HEARTBEAT:
      p->last_hb = time(NULL);
      /* If sender is the leader, update our leader heartbeat */
      if (p->id == g_leader_id)
        g_last_ldr_hb = time(NULL);
      break;

    case MSG_CPU_REPORT: {
      CpuReport *r = (CpuReport *)pl;
      float s = 0.0f;
      for (uint32_t i = 0; i < r->num_threads; i++)
        s += r->usage[i];
      p->cpu_avg = s / (float)r->num_threads;
      break;
    }

    case MSG_ELECTION: {
      /* A lower-priority node is asking for election */
      NodeInfoMsg *m = (NodeInfoMsg *)pl;
      if (m->node_id < g_my_id) {
        /* Tell them to back off */
        NodeInfoMsg me = {g_my_id, ""};
        strncpy(me.ip, g_my_ip, 15);
        peer_send_locked(p, MSG_ELECTION_OK, &me, sizeof me);
        /* Start our own election if not already */
        start_election();
      }
      break;
    }

    case MSG_ELECTION_OK:
      /* A higher-priority node is alive; back off */
      pthread_mutex_lock(&g_elec_mu);
      g_elec_ok_seen = 1;
      g_role = FOLLOWER;
      pthread_mutex_unlock(&g_elec_mu);
      grid_log(C_YELLOW, "<CONSENSUS> Yielding to dominant node %s", p->ip);
      break;

    case MSG_LEADER_ANN: {
      NodeInfoMsg *m = (NodeInfoMsg *)pl;
      g_leader_id = m->node_id;
      g_last_ldr_hb = time(NULL);
      pthread_mutex_lock(&g_elec_mu);
      g_elec_active = 0;
      if (g_role != LEADER)
        g_role = FOLLOWER;
      pthread_mutex_unlock(&g_elec_mu);
      grid_log(C_GREEN, "<CONSENSUS> Apex registered: %s", m->ip);

      /* If we have an active job, tell the new leader */
      pthread_mutex_lock(&g_worker_mu);
      uint64_t jid = g_my_job_id;
      uint8_t jtyp = g_my_job_type;
      char *jpay = g_my_job_payload;
      uint32_t jplen = g_my_job_plen;
      pthread_mutex_unlock(&g_worker_mu);

      if (jid != 0 && jpay != NULL) {
        size_t sz = sizeof(WorkerStatusMsg) + jplen;
        char *buf = malloc(sz);
        WorkerStatusMsg *ws = (WorkerStatusMsg *)buf;
        ws->job_id = jid;
        ws->job_type = jtyp;
        ws->payload_len = jplen;
        memcpy(buf + sizeof(WorkerStatusMsg), jpay, jplen);
        /* Find new leader peer */
        pthread_mutex_lock(&g_peers_mu);
        int lidx = find_peer_idx(m->node_id);
        if (lidx >= 0 && g_peers[lidx].fd >= 0)
          peer_send_locked(&g_peers[lidx], MSG_WORKER_STATUS, buf,
                           (uint32_t)sz);
        pthread_mutex_unlock(&g_peers_mu);
        free(buf);
      }
      break;
    }

    case MSG_JOB_ASSIGN: {
      /* Leader is asking us to run a job */
      JobAssignMsg *ja = (JobAssignMsg *)pl;
      uint64_t jid = ja->job_id;
      uint8_t jtype = ja->job_type;
      uint32_t clen = ja->code_len;
      char *code = pl + sizeof(JobAssignMsg);

      /* Save for potential leader-resume reporting */
      pthread_mutex_lock(&g_worker_mu);
      g_my_job_id = jid;
      g_my_job_type = jtype;
      if (g_my_job_payload)
        free(g_my_job_payload);
      g_my_job_payload = malloc(clen);
      memcpy(g_my_job_payload, code, clen);
      g_my_job_plen = clen;
      pthread_mutex_unlock(&g_worker_mu);

      grid_log(C_CYAN, "<WORKER> Processing execution order for task %llu",
               (unsigned long long)jid);
      execute_job(jid, jtype, code, clen, /*is_local=*/0, /*disp_fd=*/-1);
      break;
    }

    case MSG_STREAM_IN:
      /* Leader forwarding stdin to us */
      pthread_mutex_lock(&g_worker_mu);
      if (g_active_stdin_fd >= 0)
        write(g_active_stdin_fd, pl, hdr.payload_len);
      pthread_mutex_unlock(&g_worker_mu);
      break;

    /* ── These come from workers and are handled by the leader ── */
    case MSG_TAGGED_OUT: {
      if (g_role != LEADER)
        break;
      TaggedHdr *th = (TaggedHdr *)pl;
      char *data = pl + sizeof(TaggedHdr);
      uint32_t dlen = hdr.payload_len - sizeof(TaggedHdr);
      pthread_mutex_lock(&g_jobs_mu);
      Job *j = find_job(th->job_id);
      if (j)
        send_msg(j->disp_fd, MSG_STREAM_OUT, data, dlen);
      pthread_mutex_unlock(&g_jobs_mu);
      break;
    }
    case MSG_TAGGED_DONE: {
      if (g_role != LEADER)
        break;
      TaggedHdr *th = (TaggedHdr *)pl;
      pthread_mutex_lock(&g_jobs_mu);
      Job *j = find_job(th->job_id);
      if (j) {
        send_msg(j->disp_fd, MSG_JOB_DONE, NULL, 0);
        j->active = 0;
        if (j->payload) {
          free(j->payload);
          j->payload = NULL;
        }
      }
      pthread_mutex_unlock(&g_jobs_mu);
      check_and_dequeue();
      break;
    }
    case MSG_TAGGED_ERR: {
      if (g_role != LEADER)
        break;
      TaggedHdr *th = (TaggedHdr *)pl;
      char *data = pl + sizeof(TaggedHdr);
      uint32_t dlen = hdr.payload_len - sizeof(TaggedHdr);
      pthread_mutex_lock(&g_jobs_mu);
      Job *j = find_job(th->job_id);
      if (j) {
        send_msg(j->disp_fd, MSG_EXEC_RESULT, data, dlen);
        j->active = 0;
        if (j->payload) {
          free(j->payload);
          j->payload = NULL;
        }
      }
      pthread_mutex_unlock(&g_jobs_mu);
      check_and_dequeue();
      break;
    }

    case MSG_WORKER_STATUS: {
      /* A worker is telling us (new leader) about its live job.
         We don't have the dispatcher's connection, so we just log it
         and let the worker finish; results will be discarded unless
         the dispatcher reconnects and resubmits. */
      if (g_role != LEADER)
        break;
      WorkerStatusMsg *ws = (WorkerStatusMsg *)pl;
      grid_log(C_YELLOW, "<RECOVERY> Node %s actively executing inherited task %llu (type %d)",
               p->ip, (unsigned long long)ws->job_id, ws->job_type);
      /* Create a phantom job entry so TAGGED_DONE doesn't crash */
      pthread_mutex_lock(&g_jobs_mu);
      Job *phantom = calloc(1, sizeof(Job));
      phantom->id = ws->job_id;
      phantom->disp_fd = -1; /* no dispatcher yet */
      phantom->worker_id = p->id;
      phantom->active = 1;
      phantom->next = g_jobs;
      g_jobs = phantom;
      pthread_mutex_unlock(&g_jobs_mu);
      break;
    }

    default:
      break;
    }

    if (pl)
      free(pl);
  }

  /* Peer disconnected */
  uint32_t dead_id = p->id;
  grid_log(C_RED, "<LINK> Connection severed: %s", p->ip);
  write_ledger("PEER_DISCONNECT", p->ip, "Peer left the grid");
  kill_peer(pidx);
  p->slot_used = 0;
  g_npeers--;

  if (dead_id == g_leader_id && g_role != LEADER) {
    grid_log(C_YELLOW, "<CONSENSUS> Apex dropped! Triggering re-election...");
    start_election();
  } else if (g_role == LEADER) {
    reassign_jobs_from(dead_id);
  }
  return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════════
   Dispatcher (sender) message loop — only runs on leader
   ══════════════════════════════════════════════════════════════════════════════
 */
typedef struct {
  int fd;
  char ip[16];
} DispArg;

static void *dispatcher_loop_thread(void *arg) {
  DispArg *da = (DispArg *)arg;
  int fd = da->fd;
  char ip[16];
  strncpy(ip, da->ip, 15);
  free(da);

  pthread_mutex_lock(&g_disp_mu);
  DispClient *dc = add_disp(fd, ip);
  pthread_mutex_unlock(&g_disp_mu);

  grid_log(C_BLUE, "<GATEWAY> Ingress connection established: %s", ip);
  write_ledger("SENDER_CONNECT", ip, "Dispatcher connected");

  while (1) {
    MsgHeader hdr;
    if (recv_hdr(fd, &hdr) < 0)
      break;
    if (strcmp(hdr.auth_token, AUTH_TOKEN) != 0) {
      send_msg(fd, MSG_REJECTED, "Invalid auth", 12);
      break;
    }

    char *buf = NULL;
    if (hdr.payload_len > 0) {
      buf = malloc(hdr.payload_len + 1);
      if (recv_all(fd, buf, hdr.payload_len) < 0) {
        free(buf);
        break;
      }
      buf[hdr.payload_len] = '\0';
    }

    pthread_mutex_lock(&g_disp_mu);
    int is_banned = dc && dc->is_banned;
    pthread_mutex_unlock(&g_disp_mu);
    if (is_banned) {
      if (buf)
        free(buf);
      continue;
    }

    if (hdr.type == MSG_EXEC_REQ || hdr.type == MSG_PROJECT_REQ) {
      int is_project = (hdr.type == MSG_PROJECT_REQ);

      /* Security check (skip for binary archives) */
      if (!is_project && scan_for_malware(buf)) {
        pthread_mutex_lock(&g_disp_mu);
        if (dc)
          dc->strikes++;
        int strikes = dc ? dc->strikes : 0;
        pthread_mutex_unlock(&g_disp_mu);

        grid_log(C_RED, "<DEFENSE> VIOLATION LOGGED: %s (%d/3)", ip, strikes);
        write_ledger("STRIKE", ip, "Malicious payload detected");

        if (strikes >= MAX_STRIKES) {
          pthread_mutex_lock(&g_disp_mu);
          if (dc)
            dc->is_banned = 1;
          pthread_mutex_unlock(&g_disp_mu);
          write_ledger("BANNED", ip, "Max strikes reached");
          send_msg(fd, MSG_REJECTED, "BANNED FOR MALWARE", 18);
        } else {
          send_msg(fd, MSG_STRIKE, "Security violation logged.", 26);
        }
        if (buf)
          free(buf);
        continue;
      }

      pthread_mutex_lock(&g_jobs_mu);
      Job *j = new_job(fd, is_project ? MSG_PROJECT_WORK : MSG_EXEC_WORK, buf,
                       hdr.payload_len);
      assign_job(j);
      pthread_mutex_unlock(&g_jobs_mu);

    } else if (hdr.type == MSG_STREAM_IN) {
      /* Forward stdin to whoever is executing the job for this dispatcher */
      pthread_mutex_lock(&g_jobs_mu);
      for (Job *j = g_jobs; j; j = j->next) {
        if (!j->active || j->disp_fd != fd)
          continue;
        if (j->worker_id == g_my_id) {
          /* local job: write to child stdin directly */
          pthread_mutex_lock(&g_worker_mu);
          if (g_active_stdin_fd >= 0)
            write(g_active_stdin_fd, buf, hdr.payload_len);
          pthread_mutex_unlock(&g_worker_mu);
        } else {
          /* remote worker: forward MSG_STREAM_IN via peer fd */
          pthread_mutex_lock(&g_peers_mu);
          int widx = find_peer_idx(j->worker_id);
          if (widx >= 0 && g_peers[widx].fd >= 0)
            peer_send_locked(&g_peers[widx], MSG_STREAM_IN, buf,
                             hdr.payload_len);
          pthread_mutex_unlock(&g_peers_mu);
        }
        break;
      }
      pthread_mutex_unlock(&g_jobs_mu);
    }

    if (buf)
      free(buf);
  }

  /* Dispatcher disconnected: cancel its pending jobs */
  pthread_mutex_lock(&g_jobs_mu);
  for (Job *j = g_jobs; j; j = j->next) {
    if (j->active && j->disp_fd == fd) {
      j->active = 0;
    }
  }
  pthread_mutex_unlock(&g_jobs_mu);

  pthread_mutex_lock(&g_disp_mu);
  remove_disp(fd);
  pthread_mutex_unlock(&g_disp_mu);
  close(fd);
  grid_log(C_YELLOW, "<GATEWAY> Connection severed: %s", ip);
  return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════════
   Incoming TCP connection handler — identifies peer vs. dispatcher
   ══════════════════════════════════════════════════════════════════════════════
 */
typedef struct {
  int fd;
  char ip[16];
} IncomingArg;

static void *incoming_conn_thread(void *arg) {
  IncomingArg *ia = (IncomingArg *)arg;
  int fd = ia->fd;
  char ip[16];
  strncpy(ip, ia->ip, 15);
  free(ia);

  MsgHeader hdr;
  if (recv_hdr(fd, &hdr) < 0) {
    close(fd);
    return NULL;
  }

  if (strcmp(hdr.auth_token, P2P_TOKEN) == 0 && hdr.type == MSG_PEER_HELLO) {
    /* ── Peer connection ── */
    char *pl = NULL;
    if (hdr.payload_len > 0) {
      pl = malloc(hdr.payload_len + 1);
      recv_all(fd, pl, hdr.payload_len);
      pl[hdr.payload_len] = '\0';
    }
    NodeInfoMsg *ni = (NodeInfoMsg *)pl;
    uint32_t peer_id = ni ? ni->node_id : 0;
    const char *peer_ip = ni ? ni->ip : ip;

    /* Send back our own hello */
    NodeInfoMsg me = {g_my_id, ""};
    strncpy(me.ip, g_my_ip, 15);
    send_p2p(fd, MSG_PEER_HELLO, &me, sizeof me);

    int idx = upsert_peer(peer_id, peer_ip, fd);
    if (pl)
      free(pl);
    if (idx < 0) {
      close(fd);
      return NULL;
    }

    /* Announce role if we're leader */
    if (g_role == LEADER) {
      NodeInfoMsg ldr = {g_my_id, ""};
      strncpy(ldr.ip, g_my_ip, 15);
      peer_send_locked(&g_peers[idx], MSG_LEADER_ANN, &ldr, sizeof ldr);
    }

    PeerLoopArg *pla = malloc(sizeof(PeerLoopArg));
    pla->peer_idx = idx;
    pthread_t tid;
    pthread_create(&tid, NULL, peer_loop_thread, pla);
    pthread_detach(tid);

  } else if (strcmp(hdr.auth_token, AUTH_TOKEN) == 0 && hdr.type == MSG_AUTH) {
    /* ── Dispatcher (sender) connection ── */
    /* Drain the auth payload */
    if (hdr.payload_len > 0) {
      char *tmp = malloc(hdr.payload_len);
      recv_all(fd, tmp, hdr.payload_len);
      free(tmp);
    }

    if (g_role != LEADER) {
      /* Redirect to the known leader */
      RedirectMsg redir;
      memset(&redir, 0, sizeof redir);
      pthread_mutex_lock(&g_peers_mu);
      int lidx = find_peer_idx(g_leader_id);
      if (lidx >= 0)
        strncpy(redir.leader_ip, g_peers[lidx].ip, 15);
      else
        snprintf(redir.leader_ip, sizeof redir.leader_ip, "unknown");
      pthread_mutex_unlock(&g_peers_mu);
      send_msg(fd, MSG_REDIRECT, &redir, sizeof redir);
      close(fd);
      return NULL;
    }

    DispArg *da = malloc(sizeof(DispArg));
    da->fd = fd;
    strncpy(da->ip, ip, 15);
    pthread_t tid;
    pthread_create(&tid, NULL, dispatcher_loop_thread, da);
    pthread_detach(tid);

  } else {
    send_msg(fd, MSG_REJECTED, "Bad auth", 8);
    close(fd);
  }
  return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════════
   TCP accept thread
   ══════════════════════════════════════════════════════════════════════════════
 */
static void *accept_thread(void *arg) {
  (void)arg;
  while (1) {
    struct sockaddr_in caddr;
    socklen_t clen = sizeof caddr;
    int cfd = accept(g_listen_fd, (struct sockaddr *)&caddr, &clen);
    if (cfd < 0)
      continue;

    IncomingArg *ia = malloc(sizeof(IncomingArg));
    ia->fd = cfd;
    inet_ntop(AF_INET, &caddr.sin_addr, ia->ip, 16);
    pthread_t tid;
    pthread_create(&tid, NULL, incoming_conn_thread, ia);
    pthread_detach(tid);
  }
  return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════════
   Initiate outbound peer connection (called from UDP discovery)
   ══════════════════════════════════════════════════════════════════════════════
 */
static void connect_to_peer(uint32_t peer_id, const char *peer_ip) {
  if (peer_id == g_my_id)
    return;

  pthread_mutex_lock(&g_peers_mu);
  int already = find_peer_idx(peer_id);
  pthread_mutex_unlock(&g_peers_mu);
  if (already >= 0)
    return; /* already connected */

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons(PORT)};
  inet_pton(AF_INET, peer_ip, &sa.sin_addr);

  if (connect(fd, (struct sockaddr *)&sa, sizeof sa) < 0) {
    close(fd);
    return;
  }

  /* Send peer hello */
  NodeInfoMsg me = {g_my_id, ""};
  strncpy(me.ip, g_my_ip, 15);
  send_p2p(fd, MSG_PEER_HELLO, &me, sizeof me);

  /* Read back their hello */
  MsgHeader hdr;
  if (recv_hdr(fd, &hdr) < 0) {
    close(fd);
    return;
  }
  if (hdr.type != MSG_PEER_HELLO) {
    close(fd);
    return;
  }
  char *pl = malloc(hdr.payload_len + 1);
  recv_all(fd, pl, hdr.payload_len);
  NodeInfoMsg *their = (NodeInfoMsg *)pl;
  uint32_t their_id = their->node_id;
  char their_ip[16];
  strncpy(their_ip, their->ip, 15);
  their_ip[15] = '\0';
  free(pl);

  int idx = upsert_peer(their_id, their_ip, fd);
  if (idx < 0) {
    close(fd);
    return;
  }

  grid_log(C_BLUE, "<PROBE> Sub-net peer resolved: %s", their_ip);
  write_ledger("PEER_CONNECT", their_ip, "Outbound peer TCP established");

  /* If they're the leader, accept them */
  /* (will receive MSG_LEADER_ANN from incoming_conn_thread or via
   * MSG_HEARTBEAT) */

  PeerLoopArg *pla = malloc(sizeof(PeerLoopArg));
  pla->peer_idx = idx;
  pthread_t tid;
  pthread_create(&tid, NULL, peer_loop_thread, pla);
  pthread_detach(tid);
}

/* ══════════════════════════════════════════════════════════════════════════════
   Peer connect thread  (called from UDP discovery, avoids blocking)
   ══════════════════════════════════════════════════════════════════════════════
 */
void *peer_connect_thread(void *arg) {
  NodeInfoMsg *ni = (NodeInfoMsg *)arg;
  connect_to_peer(ni->node_id, ni->ip);
  free(ni);
  return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════════
   UDP multicast discovery
   ══════════════════════════════════════════════════════════════════════════════
 */
static void *udp_discovery_thread(void *arg) {
  (void)arg;

  /* Sender socket */
  int tx = socket(AF_INET, SOCK_DGRAM, 0);
  struct sockaddr_in dst = {.sin_family = AF_INET,
                            .sin_port = htons(MCAST_PORT)};
  inet_pton(AF_INET, MCAST_GROUP, &dst.sin_addr);

  /* Receiver socket */
  int rx = socket(AF_INET, SOCK_DGRAM, 0);
  int opt = 1;
  setsockopt(rx, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);
  struct sockaddr_in bind_addr = {.sin_family = AF_INET,
                                  .sin_port = htons(MCAST_PORT),
                                  .sin_addr.s_addr = INADDR_ANY};
  bind(rx, (struct sockaddr *)&bind_addr, sizeof bind_addr);

  struct ip_mreq mreq;
  inet_pton(AF_INET, MCAST_GROUP, &mreq.imr_multiaddr);
  mreq.imr_interface.s_addr = INADDR_ANY;
  setsockopt(rx, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof mreq);

  /* Non-blocking receive */
  struct timeval tv = {1, 0};
  setsockopt(rx, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);

  BeaconMsg beacon = {g_my_id, ""};
  strncpy(beacon.ip, g_my_ip, 15);

  time_t last_sent = 0;
  while (1) {
    /* Send beacon every BEACON_INTERVAL seconds */
    time_t now = time(NULL);
    if (now - last_sent >= BEACON_INTERVAL) {
      sendto(tx, &beacon, sizeof beacon, 0, (struct sockaddr *)&dst,
             sizeof dst);
      last_sent = now;
    }

    /* Receive beacons from others */
    BeaconMsg incoming;
    ssize_t n = recv(rx, &incoming, sizeof incoming, 0);
    if (n == sizeof(BeaconMsg) && incoming.node_id != g_my_id) {
      pthread_mutex_lock(&g_peers_mu);
      int exists = find_peer_idx(incoming.node_id) >= 0;
      pthread_mutex_unlock(&g_peers_mu);
      if (!exists) {
        /* Spawn outbound TCP connect in a thread (non-blocking discovery) */
        NodeInfoMsg *pca = malloc(sizeof(NodeInfoMsg));
        pca->node_id = incoming.node_id;
        strncpy(pca->ip, incoming.ip, 15);
        pca->ip[15] = '\0';
        pthread_t tid;
        pthread_create(&tid, NULL, peer_connect_thread, pca);
        pthread_detach(tid);
      }
    }
  }
  return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════════
   Heartbeat sender thread
   ══════════════════════════════════════════════════════════════════════════════
 */
static void *heartbeat_thread(void *arg) {
  (void)arg;
  while (1) {
    sleep(HB_INTERVAL);
    broadcast_peers(MSG_HEARTBEAT, NULL, 0);
  }
  return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════════
   Peer monitor thread  (death detection + election triggering)
   ══════════════════════════════════════════════════════════════════════════════
 */
static void *monitor_thread(void *arg) {
  (void)arg;
  while (1) {
    sleep(1);
    time_t now = time(NULL);

    /* Leader liveness check (for followers) */
    if (g_role == FOLLOWER && g_leader_id != 0 && g_last_ldr_hb != 0 &&
        now - g_last_ldr_hb > PEER_DEAD_SECS) {
      grid_log(C_YELLOW,
               "<WATCHDOG> Apex signal absent. Assuming command vacuum.");
      g_leader_id = 0;
      g_last_ldr_hb = 0;
      start_election();
    }

    /* Per-peer timeout */
    pthread_mutex_lock(&g_peers_mu);
    for (int i = 0; i < MAX_PEERS; i++) {
      if (!g_peers[i].slot_used || g_peers[i].fd < 0)
        continue;
      if (now - g_peers[i].last_hb > PEER_DEAD_SECS) {
        uint32_t dead_id = g_peers[i].id;
        char dead_ip[16];
        strncpy(dead_ip, g_peers[i].ip, 15);
        grid_log(C_YELLOW, "<WATCHDOG> Remote node %s timed out.", dead_ip);
        kill_peer(i);
        g_peers[i].slot_used = 0;
        g_npeers--;
        pthread_mutex_unlock(&g_peers_mu);

        if (dead_id == g_leader_id && g_role != LEADER) {
          grid_log(C_YELLOW,
                   "<WATCHDOG> Timeout mapped to apex node. Forcing re-election.");
          start_election();
        } else if (g_role == LEADER) {
          reassign_jobs_from(dead_id);
        }
        pthread_mutex_lock(&g_peers_mu);
      }
    }
    pthread_mutex_unlock(&g_peers_mu);
  }
  return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════════
   CPU reporter thread (sends utilisation to leader every second)
   ══════════════════════════════════════════════════════════════════════════════
 */
typedef struct {
  unsigned long long user, sys, idle, total;
} CpuTick;
static int read_cpu_ticks(CpuTick *ticks, int max) {
  FILE *f = fopen("/proc/stat", "r");
  if (!f)
    return 0;
  char line[256];
  int n = 0;
  while (fgets(line, sizeof line, f) && n < max) {
    if (strncmp(line, "cpu", 3) != 0)
      continue;
    if (line[3] < '0' || line[3] > '9')
      continue;
    unsigned long long u, ni, s, id, iow, irq, si, st;
    if (sscanf(line + 3, "%*u %llu %llu %llu %llu %llu %llu %llu %llu", &u, &ni,
               &s, &id, &iow, &irq, &si, &st) != 8)
      continue;
    ticks[n].user = u + ni;
    ticks[n].sys = s + irq + si + st;
    ticks[n].idle = id + iow;
    ticks[n].total = ticks[n].user + ticks[n].sys + ticks[n].idle;
    n++;
  }
  fclose(f);
  return n;
}

static void *cpu_reporter_thread(void *arg) {
  (void)arg;
  CpuTick prev[MAX_THREADS], curr[MAX_THREADS];
  memset(prev, 0, sizeof prev);
  read_cpu_ticks(prev, MAX_THREADS);
  while (1) {
    sleep(1);
    int nc = read_cpu_ticks(curr, MAX_THREADS);
    if (nc <= 0)
      continue;
    CpuReport rpt;
    rpt.num_threads = (uint32_t)nc;
    for (int i = 0; i < nc; i++) {
      unsigned long long dt = curr[i].total - prev[i].total;
      unsigned long long di = curr[i].idle - prev[i].idle;
      rpt.usage[i] = (dt == 0) ? 0.0f : 100.0f * (float)(dt - di) / (float)dt;
      if (rpt.usage[i] < 0)
        rpt.usage[i] = 0;
      if (rpt.usage[i] > 100)
        rpt.usage[i] = 100;
      prev[i] = curr[i];
    }
    /* Send to leader if we're a follower */
    if (g_role != LEADER && g_leader_id != 0) {
      pthread_mutex_lock(&g_peers_mu);
      int lidx = find_peer_idx(g_leader_id);
      if (lidx >= 0 && g_peers[lidx].fd >= 0)
        peer_send_locked(&g_peers[lidx], MSG_CPU_REPORT, &rpt, sizeof rpt);
      pthread_mutex_unlock(&g_peers_mu);
    }
  }
  return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════════
   TUI render thread
   ══════════════════════════════════════════════════════════════════════════════
 */
static void *tui_thread(void *arg) {
  (void)arg;
  printf("\x1b[2J"); /* clear screen once */
  while (1) {
    sleep(1);
    render_tui();
  }
  return NULL;
}

static void handle_sigint(int sig) {
  (void)sig;
  printf("\x1b[?25h\x1b[2J\x1b[H");
  printf(C_GREEN "Daemon terminated. Audit log persisted to grid_ledger.csv\n" C_RESET);
  exit(0);
}

/* ══════════════════════════════════════════════════════════════════════════════
   main
   ══════════════════════════════════════════════════════════════════════════════
 */
int main(void) {
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, handle_sigint);

  /* Identify ourselves */
  g_my_id = get_my_ip(g_my_ip);
  memset(g_peers, 0, sizeof g_peers);
  memset(g_disps, 0, sizeof g_disps);

  /* TCP listen socket */
  g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  int opt = 1;
  setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);
  struct sockaddr_in addr = {.sin_family = AF_INET,
                             .sin_port = htons(PORT),
                             .sin_addr.s_addr = INADDR_ANY};
  if (bind(g_listen_fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
    perror("bind");
    return 1;
  }
  listen(g_listen_fd, SOMAXCONN);

  printf(C_BG_MAGENTA C_BOLD
         "\n  ==[ OS MINI PROJECT ]==  BIND: %s  Booting sequence...\n\n" C_RESET,
         g_my_ip);

  /* Assume we might be the only node → try to become leader after a brief
     discovery window. Any node that was already running will announce itself.
   */
  g_last_ldr_hb = time(NULL); /* don't trigger election immediately */

  pthread_t t1, t2, t3, t4, t5, t6;
  pthread_create(&t1, NULL, accept_thread, NULL);
  pthread_create(&t2, NULL, udp_discovery_thread, NULL);
  pthread_create(&t3, NULL, heartbeat_thread, NULL);
  pthread_create(&t4, NULL, monitor_thread, NULL);
  pthread_create(&t5, NULL, cpu_reporter_thread, NULL);
  pthread_create(&t6, NULL, tui_thread, NULL);

  /* Wait a bit for peer discovery, then start election if no leader yet */
  sleep(2);
  if (g_leader_id == 0) {
    grid_log(C_YELLOW, "<INIT> Apex node absent. Triggering consensus...");
    start_election();
  }

  /* Main thread: just joins to keep process alive */
  pthread_join(t1, NULL);
  return 0;
}