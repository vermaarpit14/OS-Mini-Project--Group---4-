#ifndef COMMON_H
#define COMMON_H

#include <stdint.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>

/* ── ANSI colours ───────────────────────────────────────────────────────────
 */
#define C_RESET "\x1b[0m"
#define C_BOLD "\x1b[1m"
#define C_RED "\x1b[31m"
#define C_GREEN "\x1b[32m"
#define C_YELLOW "\x1b[33m"
#define C_BLUE "\x1b[34m"
#define C_MAGENTA "\x1b[35m"
#define C_CYAN "\x1b[36m"
#define C_BG_BLUE "\x1b[44m"

/* ── Tunables ───────────────────────────────────────────────────────────────
 */
#define PORT 8080
#define MCAST_PORT 9090
#define MCAST_GROUP "239.0.0.1"
#define MAX_CLIENTS 64
#define MAX_THREADS 64
#define MAX_RESULT_TEXT (64 * 1024)
#define MAX_QUEUE_SIZE 100
#define AUTH_TOKEN "N1T_CSE_SECURE_77" /* dispatcher → node  */
#define P2P_TOKEN "GRID_P2P_NODE_AUTH" /* node → node        */
#define C_BG_MAGENTA "\x1b[45m"
#define C_WHITE      "\x1b[97m"

/* ── Message catalogue ──────────────────────────────────────────────────────
 */
typedef enum {
  /* ── original dispatcher ↔ leader messages ── */
  MSG_CPU_REPORT = 1,
  MSG_EXEC_REQ = 2,
  MSG_EXEC_WORK = 3,
  MSG_EXEC_RESULT = 4,
  MSG_AUTH = 5,
  MSG_REJECTED = 6,
  MSG_STREAM_IN = 7,
  MSG_STREAM_OUT = 8,
  MSG_JOB_DONE = 9,
  MSG_STRIKE = 10,
  MSG_PROJECT_REQ = 11,
  MSG_PROJECT_WORK = 12,
  /* ── P2P peer messages ── */
  MSG_PEER_HELLO = 13,    /* TCP handshake between nodes             */
  MSG_HEARTBEAT = 14,     /* periodic liveness ping                  */
  MSG_ELECTION = 15,      /* "start election" (bully)                */
  MSG_ELECTION_OK = 16,   /* "I'm higher priority, back off"         */
  MSG_LEADER_ANN = 17,    /* "I am the new leader"                   */
  MSG_JOB_ASSIGN = 18,    /* leader → worker: execute this job       */
  MSG_TAGGED_OUT = 19,    /* worker → leader: stdout chunk + job_id  */
  MSG_TAGGED_DONE = 20,   /* worker → leader: job finished normally  */
  MSG_TAGGED_ERR = 21,    /* worker → leader: job error/signal       */
  MSG_REDIRECT = 22,      /* non-leader → dispatcher: go talk to X  */
  MSG_WORKER_STATUS = 23, /* worker → new leader: "I have a live job"*/
} MsgType;

/* ── Wire-format structures (packed, no padding) ────────────────────────────
 */
#pragma pack(push, 1)

/* Every TCP message starts with this header */
typedef struct {
  uint8_t type;
  uint32_t payload_len;
  char auth_token[32];
} MsgHeader;

/* CPU utilisation report (worker → leader) */
typedef struct {
  uint32_t num_threads;
  float usage[MAX_THREADS];
} CpuReport;

/* Peer handshake / election / leader announcement */
typedef struct {
  uint32_t node_id;
  char ip[16];
} NodeInfoMsg;

/* Leader → worker: run a job.
   The JobAssignMsg is followed immediately by code_len bytes of payload. */
typedef struct {
  uint64_t job_id;
  uint8_t job_type; /* MSG_EXEC_WORK or MSG_PROJECT_WORK */
  uint32_t code_len;
} JobAssignMsg;

/* Worker → leader: tagged output / done / error.
   TaggedHdr is followed by data bytes (for TAGGED_OUT / TAGGED_ERR). */
typedef struct {
  uint64_t job_id;
} TaggedHdr;

/* Node → dispatcher: redirect to leader */
typedef struct {
  char leader_ip[16];
} RedirectMsg;

/* Worker → new leader after election: "I'm still running job X" */
typedef struct {
  uint64_t job_id;
  uint8_t job_type;
  uint32_t payload_len;
  /* followed by payload_len bytes */
} WorkerStatusMsg;

/* UDP multicast discovery beacon */
typedef struct {
  uint32_t node_id;
  char ip[16];
} BeaconMsg;

#pragma pack(pop)

/* ── Inline send / recv helpers ─────────────────────────────────────────────
 */
static inline int send_all(int fd, const void *buf, size_t len) {
  const char *p = (const char *)buf;
  while (len > 0) {
    ssize_t n = send(fd, p, len, 0);
    if (n <= 0)
      return -1;
    p += n;
    len -= (size_t)n;
  }
  return 0;
}

static inline int recv_all(int fd, void *buf, size_t len) {
  char *p = (char *)buf;
  while (len > 0) {
    ssize_t n = recv(fd, p, len, 0);
    if (n <= 0)
      return -1;
    p += n;
    len -= (size_t)n;
  }
  return 0;
}

/* Send a message tagged for dispatcher auth */
static inline int send_msg(int fd, MsgType type, const void *payload,
                           uint32_t plen) {
  MsgHeader hdr = {(uint8_t)type, plen, ""};
  snprintf(hdr.auth_token, sizeof(hdr.auth_token), "%s", AUTH_TOKEN);
  if (send_all(fd, &hdr, sizeof hdr) < 0)
    return -1;
  if (plen && payload && send_all(fd, payload, plen) < 0)
    return -1;
  return 0;
}

/* Send a message tagged for P2P auth */
static inline int send_p2p(int fd, MsgType type, const void *payload,
                           uint32_t plen) {
  MsgHeader hdr = {(uint8_t)type, plen, ""};
  snprintf(hdr.auth_token, sizeof(hdr.auth_token), "%s", P2P_TOKEN);
  if (send_all(fd, &hdr, sizeof hdr) < 0)
    return -1;
  if (plen && payload && send_all(fd, payload, plen) < 0)
    return -1;
  return 0;
}

static inline int recv_hdr(int fd, MsgHeader *hdr) {
  return recv_all(fd, hdr, sizeof *hdr);
}

#endif /* COMMON_H */
