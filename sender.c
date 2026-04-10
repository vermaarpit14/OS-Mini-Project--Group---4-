/*
 * sender.c  —  CODE Submission / Console
 * ─────────────────────────────────────────────────────────────────────────────
 * Connects to any grid node; if that node is not the leader it will receive a
 * MSG_REDIRECT pointing to the current leader and transparently reconnect.
 *
 * If the connection drops mid-job (leader died + election happened), the
 * console prints a warning and offers to resubmit the last job automatically
 * once it reconnects to the new leader.
 *
 * [UPGRADE 1] On startup, prompts for a "Cluster Secret" with echo disabled.
 * The secret is embedded in the MSG_AUTH payload (AuthPayload).
 * The leader drops the connection if the secret is wrong.
 *
 * [UPGRADE 2] Typing "local <path>" forces execution on the leader node,
 * bypassing the load-balancer.  The path can be a .c file or a
 * project directory, exactly like a normal submission.
 * ─────────────────────────────────────────────────────────────────────────────
 */
#include "common.h"

#include <arpa/inet.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <termios.h>   /* [UPGRADE 1] for echo-suppressed password input */
#include <unistd.h>

#define STATE_IDLE 0
#define STATE_BUSY 1
#define MAX_RECONNECT_TRIES 5
#define RECONNECT_DELAY_SEC 2

/* ── Path / payload remembered for resubmission ─────────────────────────── */
static char  g_last_path[1024]   = {0};
static char *g_last_payload      = NULL;
static long  g_last_payload_sz   = 0;
static int   g_last_is_project   = 0;
static int   g_last_force_local  = 0; /* [UPGRADE 2] was it a "local" job?  */

/*
 * [UPGRADE 1] Shared cluster secret collected once at startup.
 * Stored in process memory for the lifetime of the session.
 */
static char g_cluster_secret[64] = {0};

/* ── Helpers ─────────────────────────────────────────────────────────────── */

/*
 * [UPGRADE 1] Read a password from the terminal without echoing characters.
 * Falls back to a normal fgets if stdin is not a tty (e.g. in a pipe/test).
 */
static void read_secret(const char *prompt, char *out, size_t maxlen) {
  if (isatty(STDIN_FILENO)) {
    struct termios old, noecho;
    tcgetattr(STDIN_FILENO, &old);
    noecho = old;
    noecho.c_lflag &= ~(tcflag_t)ECHO; /* turn off echo */
    tcsetattr(STDIN_FILENO, TCSANOW, &noecho);

    fprintf(stderr, "%s", prompt);
    fflush(stderr);
    if (fgets(out, (int)maxlen, stdin))
      out[strcspn(out, "\n")] = '\0';
    fprintf(stderr, "\n"); /* move to new line after hidden input */

    tcsetattr(STDIN_FILENO, TCSANOW, &old); /* restore echo */
  } else {
    /* Non-interactive: just read plaintext (useful for automated tests) */
    fprintf(stderr, "%s", prompt);
    if (fgets(out, (int)maxlen, stdin))
      out[strcspn(out, "\n")] = '\0';
  }
}

static void clean_path(char *path) {
  int len = (int)strlen(path);
  while (len > 0 && (path[len-1] == '\n' || path[len-1] == '\r' ||
                     path[len-1] == ' '))
    path[--len] = '\0';
  char *start = path;
  while (*start == ' ') start++;
  if ((start[0] == '\'' && start[len-1] == '\'') ||
      (start[0] == '\"' && start[len-1] == '\"')) {
    start[len-1] = '\0';
    start++;
  }
  memmove(path, start, strlen(start) + 1);
  char *rd = path, *wr = path;
  while (*rd) {
    if (*rd == '\\' && *(rd+1) == ' ') rd++;
    *wr++ = *rd++;
  }
  *wr = '\0';
}

/*
 * Connect to ip:PORT, perform auth handshake (including cluster secret),
 * and handle redirects transparently.
 * Returns a connected+authenticated socket fd, or -1 on total failure.
 */
static int dial_leader(const char *initial_ip) {
  char cur_ip[64];
  strncpy(cur_ip, initial_ip, sizeof cur_ip - 1);

  for (int attempt = 0; attempt < MAX_RECONNECT_TRIES; attempt++) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in srv = {.sin_family = AF_INET,
                              .sin_port   = htons(PORT)};
    inet_pton(AF_INET, cur_ip, &srv.sin_addr);

    if (connect(fd, (struct sockaddr *)&srv, sizeof srv) < 0) {
      close(fd);
      printf(C_YELLOW "<LINK> Unable to contact %s, retrying in %ds...\n" C_RESET,
             cur_ip, RECONNECT_DELAY_SEC);
      sleep(RECONNECT_DELAY_SEC);
      continue;
    }

    /*
     * [UPGRADE 1] Build an AuthPayload containing "HELLO" + the cluster
     * secret and send it as the MSG_AUTH payload.  The leader's
     * incoming_conn_thread will verify CLUSTER_SECRET before accepting.
     */
    AuthPayload ap;
    memset(&ap, 0, sizeof ap);
    snprintf(ap.greeting, sizeof ap.greeting, "HELLO");
    strncpy(ap.secret, g_cluster_secret, sizeof ap.secret - 1);
    send_msg(fd, MSG_AUTH, &ap, sizeof ap);

    /* Read response — may be MSG_REDIRECT, MSG_REJECTED, or nothing */
    struct pollfd pf = {fd, POLLIN, 0};
    if (poll(&pf, 1, 1500) > 0 && (pf.revents & POLLIN)) {
      MsgHeader hdr;
      if (recv_hdr(fd, &hdr) < 0) { close(fd); continue; }

      if (hdr.type == MSG_REDIRECT) {
        RedirectMsg redir;
        if (hdr.payload_len >= sizeof redir)
          recv_all(fd, &redir, sizeof redir);
        close(fd);
        if (strcmp(redir.leader_ip, "unknown") == 0) {
          printf(C_YELLOW "<Election> Coordinator unassigned. Awaiting negotiation...\n" C_RESET);
          sleep(RECONNECT_DELAY_SEC);
          continue;
        }
        printf(C_BG_MAGENTA C_WHITE C_BOLD
               "<ROUTING> Traffic redirected to Coordinator at %s\n" C_RESET,
               redir.leader_ip);
        strncpy(cur_ip, redir.leader_ip, sizeof cur_ip - 1);
        continue;

      } else if (hdr.type == MSG_REJECTED) {
        /* [UPGRADE 1] Bad secret or banned — print reason and abort */
        char *buf = hdr.payload_len ? malloc(hdr.payload_len + 1) : NULL;
        if (buf) {
          recv_all(fd, buf, hdr.payload_len);
          buf[hdr.payload_len] = '\0';
        }
        printf(C_RED "<GATEWAY> Handshake denied: %s\n" C_RESET, buf ? buf : "");
        if (buf) free(buf);
        close(fd);
        return -1;

      } else {
        /* Some other message (e.g. initial stream fragment) — skip payload */
        if (hdr.payload_len) {
          char *tmp = malloc(hdr.payload_len);
          recv_all(fd, tmp, hdr.payload_len);
          free(tmp);
        }
      }
    }

    printf(C_BG_MAGENTA C_WHITE C_BOLD
           " === CODE UPLINK ESTABLISHED | Coordinator: %s === " C_RESET "\n",
           cur_ip);
    return fd;
  }

  printf(C_RED "<FATAL> Link establishment failed after %d cycles.\n" C_RESET,
         MAX_RECONNECT_TRIES);
  return -1;
}

/*
 * [UPGRADE 2] Helper: load a file or project directory into g_last_payload
 * and send it via the appropriate message type.
 *
 * msg_type can be MSG_EXEC_REQ, MSG_PROJECT_REQ, or MSG_LOCAL_EXEC_REQ.
 * For MSG_LOCAL_EXEC_REQ the payload format is identical to MSG_EXEC_REQ;
 * the difference is purely in the message type byte so the leader knows to
 * skip the load-balancer.
 *
 * Returns 1 on success, 0 on error (caller stays in STATE_IDLE).
 */
static int submit_path(int sockfd, const char *raw_path, int force_local) {
  char path[1024];
  strncpy(path, raw_path, sizeof path - 1);
  path[sizeof path - 1] = '\0';
  clean_path(path);

  if (strlen(path) == 0) return 0;

  struct stat st;
  if (stat(path, &st) != 0) {
    printf(C_RED "Error: Locator '%s' not found\n" C_RESET, path);
    return 0;
  }

  strncpy(g_last_path, path, sizeof g_last_path - 1);
  if (g_last_payload) { free(g_last_payload); g_last_payload = NULL; }
  g_last_force_local = force_local;

  if (S_ISDIR(st.st_mode)) {
    printf(C_YELLOW "<COMPILER> Archiving project directory...\n" C_RESET);
    char cmd[1024];
    snprintf(cmd, sizeof cmd, "tar -czf /tmp/grid_send.tar.gz -C \"%s\" .", path);
    system(cmd);
    FILE *src = fopen("/tmp/grid_send.tar.gz", "rb");
    if (!src) { printf(C_RED "Error archiving project payload.\n" C_RESET); return 0; }
    fseek(src, 0, SEEK_END);
    long sz = ftell(src); rewind(src);
    g_last_payload    = calloc(1, sz + 1);
    fread(g_last_payload, 1, sz, src);
    fclose(src);
    g_last_payload_sz = sz;
    g_last_is_project = 1;

    /*
     * [UPGRADE 2] There is no "local project" variant in the message
     * catalogue because project archives are large; local execution of a
     * project archive is still routed through MSG_PROJECT_REQ regardless
     * of force_local.  Only single .c files honour force-local.
     * This keeps the protocol simple and the node.c scheduler clean.
     */
    MsgType req = force_local ? MSG_PROJECT_REQ : MSG_PROJECT_REQ;
    printf(C_CYAN "<UPLINK> Transmitting Project Payload (%ld bytes)...\n" C_RESET, sz);
    send_msg(sockfd, req, g_last_payload, (uint32_t)sz);

  } else {
    FILE *src = fopen(path, "rb");
    if (!src) { printf(C_RED "Error accessing file '%s'\n" C_RESET, path); return 0; }
    fseek(src, 0, SEEK_END);
    long sz = ftell(src); rewind(src);
    g_last_payload    = calloc(1, sz + 1);
    fread(g_last_payload, 1, sz, src);
    fclose(src);
    g_last_payload_sz = sz;
    g_last_is_project = 0;

    /*
     * [UPGRADE 2] Use MSG_LOCAL_EXEC_REQ when the user typed "local <path>".
     * The payload is identical to MSG_EXEC_REQ; only the type byte differs.
     */
    MsgType req = force_local ? MSG_LOCAL_EXEC_REQ : MSG_EXEC_REQ;
    if (force_local)
      printf(C_CYAN "<UPLINK> [FORCE-LOCAL] Transmitting to Coordinator (%ld bytes)...\n"
             C_RESET, sz);
    else
      printf(C_CYAN "<UPLINK> Transmitting Binary Payload (%ld bytes)...\n" C_RESET, sz);
    send_msg(sockfd, req, g_last_payload, (uint32_t)sz);
  }
  return 1;
}

/* ══════════════════════════════════════════════════════════════════════════
   main
   ══════════════════════════════════════════════════════════════════════════ */
int main(void) {
  /*
   * CRITICAL FIX: Force stdout to be unbuffered so that when spawned by 
   * Node.js pipes, prompts are immediately sent without needing fflush().
   */
  setvbuf(stdout, NULL, _IONBF, 0);

  char initial_ip[64] = {0};

  printf(C_CYAN C_BOLD
         "\n++==================================================++\n"
         "  [ CODE Submission V9.1 ] Distributed Client Console  \n"
         "++==================================================++\n\n" C_RESET);

  /*
   * [UPGRADE 1] Prompt for cluster secret before doing anything else.
   * Echo is suppressed via termios so the passphrase is not visible.
   */
  read_secret(C_YELLOW "Cluster Secret: " C_RESET, g_cluster_secret,
              sizeof g_cluster_secret);

  printf(C_CYAN C_BOLD "CODE Terminal Initialized.\n" C_RESET);
  printf("Enter target node IP (e.g., 127.0.0.1): ");
  if (fgets(initial_ip, sizeof initial_ip, stdin))
    initial_ip[strcspn(initial_ip, "\n")] = '\0';
  if (strlen(initial_ip) == 0)
    strcpy(initial_ip, "127.0.0.1");

  int sockfd = dial_leader(initial_ip);
  if (sockfd < 0) return 1;

  printf(C_GREEN
         "Usage:\n"
         "  <path.c>          Submit a C file for distributed execution\n"
         "  <dir/>            Submit a project directory\n"
         "  local <path.c>    Force execution on the Coordinator node\n"
         "  exit / quit       Disconnect\n"
         C_RESET);

  int state = STATE_IDLE;

reconnect_loop:;
  struct pollfd fds[2] = {{STDIN_FILENO, POLLIN, 0}, {sockfd, POLLIN, 0}};

  while (1) {
    if (state == STATE_IDLE) {
      printf(C_MAGENTA "\nCODE> " C_RESET);
      fflush(stdout);
    }

    if (poll(fds, 2, -1) < 0) break;

    /* ── stdin input ───────────────────────────────────────────────── */
    if (fds[0].revents & POLLIN) {
      char input_buf[1024];
      ssize_t bytes = read(STDIN_FILENO, input_buf, sizeof(input_buf) - 1);
      if (bytes <= 0) break;
      input_buf[bytes] = '\0';

      if (state == STATE_IDLE) {
        /* strip trailing newline for command parsing */
        char cmd_line[1024];
        strncpy(cmd_line, input_buf, sizeof cmd_line - 1);
        cmd_line[strcspn(cmd_line, "\n\r")] = '\0';

        if (strlen(cmd_line) == 0) continue;
        if (!strcmp(cmd_line, "exit") || !strcmp(cmd_line, "quit")) break;

        /*
         * [UPGRADE 2] Detect "local <path>" prefix.
         * Everything after the keyword is treated as the file/dir path.
         */
        int force_local = 0;
        const char *path_arg = cmd_line;
        if (strncmp(cmd_line, "local ", 6) == 0) {
          force_local = 1;
          path_arg    = cmd_line + 6;
          printf(C_YELLOW
                 "<SCHEDULER> Force-local flag set. Coordinator will run this job directly.\n"
                 C_RESET);
        }

        if (submit_path(sockfd, path_arg, force_local))
          state = STATE_BUSY;

      } else { /* STATE_BUSY — forward stdin to job */
        send_msg(sockfd, MSG_STREAM_IN, input_buf, (uint32_t)bytes);
      }
    }

    /* ── network response ──────────────────────────────────────────── */
    if (fds[1].revents & POLLIN) {
      MsgHeader hdr;
      if (recv_hdr(sockfd, &hdr) < 0) {
        printf(C_RED "\n<LINK> Connection to Coordinator severed!\n" C_RESET);

        if (state == STATE_BUSY) {
          printf(C_YELLOW
                 "<RECOVERY> Task interrupted. Attempting host migration...\n" C_RESET);
          close(sockfd);
          sleep(RECONNECT_DELAY_SEC + 2);
          sockfd = dial_leader(initial_ip);
          if (sockfd < 0) break;

          if (g_last_payload && g_last_payload_sz > 0) {
            printf(C_YELLOW
                   "<RECOVERY> Uplink restored to new Coordinator.\n"
                   "       Orphaned Task: %s\n"
                   "       Initiate auto-resubmit? [y/n]: " C_RESET,
                   g_last_path);
            fflush(stdout);
            char ans[8] = {0};
            if (fgets(ans, sizeof ans, stdin) &&
                (ans[0] == 'y' || ans[0] == 'Y')) {
              printf(C_CYAN "<UPLINK> Re-transmitting task payload...\n" C_RESET);
              MsgType req;
              if (g_last_is_project)
                req = MSG_PROJECT_REQ;
              else
                /* [UPGRADE 2] preserve force-local across resubmission */
                req = g_last_force_local ? MSG_LOCAL_EXEC_REQ : MSG_EXEC_REQ;
              send_msg(sockfd, req, g_last_payload, (uint32_t)g_last_payload_sz);
              state = STATE_BUSY;
            } else {
              state = STATE_IDLE;
            }
            fds[1].fd = sockfd;
            goto reconnect_loop;
          }
          state     = STATE_IDLE;
          fds[1].fd = sockfd;
          goto reconnect_loop;
        }
        break;
      }

      char *out_buf = NULL;
      if (hdr.payload_len > 0) {
        out_buf = malloc(hdr.payload_len + 1);
        recv_all(sockfd, out_buf, hdr.payload_len);
        out_buf[hdr.payload_len] = '\0';
      }

      switch ((MsgType)hdr.type) {
      case MSG_STREAM_OUT:
        if (out_buf) { printf("%s", out_buf); fflush(stdout); }
        break;
      case MSG_JOB_DONE:
        printf(C_GREEN "\n<EXECUTION> Task successfully completed.\n" C_RESET);
        state = STATE_IDLE;
        break;
      case MSG_EXEC_RESULT:
        if (out_buf) printf(C_RED "\n<CODE Fault>: %s\n" C_RESET, out_buf);
        state = STATE_IDLE;
        break;
      case MSG_STRIKE:
        if (out_buf)
          printf(C_YELLOW "\n<DEFENSE> SECURITY VIOLATION: %s\n" C_RESET, out_buf);
        state = STATE_IDLE;
        break;
      case MSG_REJECTED:
        if (out_buf)
          printf(C_RED "\n<GATEWAY> Request Rejected: %s\n" C_RESET, out_buf);
        if (out_buf && strstr(out_buf, "BANNED")) { free(out_buf); exit(1); }
        state = STATE_IDLE;
        break;
      case MSG_REDIRECT: {
        RedirectMsg *redir = (RedirectMsg *)out_buf;
        if (redir && strcmp(redir->leader_ip, "unknown") != 0) {
          printf(C_YELLOW
                 "\n<ROUTING> Coordinator rotation detected, migrating link to %s...\n" C_RESET,
                 redir->leader_ip);
          close(sockfd);
          sockfd = dial_leader(redir->leader_ip);
          if (sockfd < 0) { if (out_buf) free(out_buf); goto done; }
          state     = STATE_IDLE;
          fds[1].fd = sockfd;
          if (out_buf) free(out_buf);
          goto reconnect_loop;
        }
        break;
      }
      default:
        break;
      }
      if (out_buf) free(out_buf);
    }
  }

done:
  close(sockfd);
  if (g_last_payload) free(g_last_payload);
  return 0;
}