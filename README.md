
# 🚀 Distributed C Execution Grid (DCEG)

<div align="center">

![C99](https://img.shields.io/badge/Language-C99-blue?style=for-the-badge&logo=c)
![Node.js](https://img.shields.io/badge/Runtime-Node.js-green?style=for-the-badge&logo=nodedotjs)
![GCC](https://img.shields.io/badge/Build-GCC-orange?style=for-the-badge)
![OS](https://img.shields.io/badge/OS-Linux%20%7C%20macOS-lightgrey?style=for-the-badge)

### High‑Performance Distributed Execution Framework for C Programs

</div>

---

## ✨ Overview

**Distributed C Execution Grid (DCEG)** is a **peer‑to‑peer distributed computing framework** designed to execute C programs efficiently across multiple machines.

It combines **distributed systems concepts**, **OS-level sandboxing**, and **modern web technologies** to provide a secure, scalable, and developer‑friendly environment for executing computational workloads.

---

## 🔥 Key Highlights

- 🌐 **UDP Multicast Auto‑Discovery** – Nodes dynamically discover peers without manual configuration
- 👑 **Bully Leader Election Algorithm** – Automatic coordinator selection and failover
- ⚖️ **Intelligent Load Balancing** – Jobs assigned to node with lowest CPU usage
- 🛡️ **Secure Execution Sandbox**
  - Prevents infinite loops using `RLIMIT_CPU`
  - Prevents memory abuse using `RLIMIT_AS`
- 🔄 **Fault Tolerant Execution**
  - Tasks automatically reassigned if worker fails
- 🖥️ **Modern Web Interface**
  - Write, compile, and execute C programs directly from browser
- ⚡ **Local Execution Override**
  - Run tasks locally bypassing scheduler

---

## 🧠 System Architecture

```
Browser UI (Socket.io)
        │
        ▼
 Node.js Server (server.js)
        │
        ▼
 Coordinator Node (node.c)
  ├── Load Balancer
  ├── Scheduler
  ├── Job Queue
        │
        ├──── Worker Node 1
        ├──── Worker Node 2
        └──── Worker Node N
```

---

## 🛠️ Tech Stack

| Component        | Technology        |
|------------------|-------------------|
| Core Language    | C (C99)           |
| Backend Runtime  | Node.js           |
| Communication    | TCP + UDP Multicast |
| Build Tool       | GCC               |
| Realtime UI      | Socket.io         |
| OS Support       | Linux / macOS     |

---

## ⚙️ Installation

### Prerequisites

- GCC Compiler
- Node.js v16+
- Linux / macOS

---

### 1️⃣ Compile Grid Programs

```bash
gcc -Wall -Wextra -O2 node.c -o node -lpthread
gcc -Wall -Wextra -O2 sender.c -o sender
```

---

### 2️⃣ Start Grid Nodes

Run multiple nodes across terminals or machines:

```bash
./node
```

First node automatically becomes **Coordinator**.

---

### 3️⃣ Start Web Interface

```bash
npm install express socket.io
node server.js
```

Open in browser:

```
http://localhost:3000
```

---

## 📖 Usage

### 🌍 Using Web Interface

1. Open browser at `localhost:3000`
2. Enter cluster secret (default: `osgroup4`)
3. Enter node IP (example: `127.0.0.1`)
4. Write C code
5. Click **Execute**
6. Enable **Force Local** to bypass load balancing

---

### 💻 Using CLI

```bash
./sender
```

| Command              | Description |
|---------------------|------------|
| `/path/file.c`      | Execute C file |
| `/path/project/`    | Execute multi‑file project |
| `local /path/file.c`| Force local execution |

---

## 🛡️ Security Model

| Risk            | Protection Mechanism |
|-----------------|---------------------|
| Infinite loop   | CPU time limit (RLIMIT_CPU) |
| Memory abuse    | Address space limit (RLIMIT_AS) |
| Node crash      | Job reassignment |
| Leader failure  | Bully election algorithm |

---

## 📊 Features Matrix

| Feature            | Supported |
|--------------------|----------|
| Peer Discovery     | ✅ |
| Leader Election    | ✅ |
| Load Balancing     | ✅ |
| Fault Tolerance    | ✅ |
| Sandbox Execution  | ✅ |
| Web UI             | ✅ |
| CLI Client         | ✅ |

---

## 👨‍💻 Authors

| Name                                   | Roll No |
|----------------------------------------|--------|
| PALURI VEERA DURGA VARA PRASAD         | 24CS8031 |
| GOGULAMUDI PREM SANTHOSH               | 24CS8032 |
| PRADIP GORAI                           | 24CS8033 |
| GUNTREDDI NEELAPRASANTH                | 24CS8034 |
| DARLA POOJITHA                         | 24CS8035 |
| ANJALI SINGH                           | 24CS8036 |
| ANIKET HALDAR                          | 24CS8037 |
| **ARPIT VERMA (Leader)**               | 24CS8038 |
| HIMANSHU GUPTA                         | 24CS8039 |
| RAJ GURU                               | 24CS8040 |

---

## 📜 License

This project is developed for academic purposes as part of an **Operating Systems Mini Project**.

---

<div align="center">

### ⭐ If you like this project, consider giving it a star!

</div>
