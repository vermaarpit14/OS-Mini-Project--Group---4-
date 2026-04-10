# ⚙️ Distributed Runtime Environment in C

### OS Mini Project (Group 4)

A lightweight **peer-to-peer distributed computing system** written in C
that allows multiple machines on the same network to automatically form
a cluster and share computational workload efficiently.

This system supports **automatic node discovery, intelligent load
balancing, leader election, and fault tolerance**, ensuring that jobs
continue running even if a node crashes.

Designed to demonstrate practical implementation of **Operating System
concepts** such as process control, IPC, networking, multithreading, and
resource management.

------------------------------------------------------------------------

# ✨ Features

## 🔍 Automatic Node Discovery

Nodes discover each other automatically using UDP multicast messages. No
manual configuration or IP hardcoding required.

## ⚖️ Intelligent Load Balancing

Tasks are assigned to the node with the lowest CPU load to improve
execution speed and resource utilization.

## ♻️ Self‑Healing System

If a worker node crashes, the system automatically reassigns the task to
another available node.

If the coordinator node stops working, the cluster elects a new
coordinator automatically.

## 🔒 Secure Execution Environment

Programs are executed in an isolated environment with system resource
limits applied to prevent misuse of CPU or memory.

## 📡 Real-Time Communication

Input and output streams are transferred live between nodes, allowing
interactive execution of programs.

------------------------------------------------------------------------

# 🧠 System Architecture

Each machine in the network runs a node which can act as:

• Coordinator -- manages task distribution\
• Worker -- executes assigned tasks

The system dynamically assigns roles depending on network conditions.

### 1. Peer Discovery

Nodes broadcast their availability using UDP multicast every few
seconds. When another node is detected, a TCP connection is established
for reliable communication.

### 2. Leader Election

When multiple nodes are available, a coordinator is selected
automatically. If the coordinator fails, another node takes control
without interrupting running jobs.

### 3. Task Scheduling

When a task is submitted, the coordinator checks CPU usage of all nodes.
The job is sent to the least busy worker node.

### 4. Execution Flow

1.  User sends C file or project folder
2.  Coordinator selects worker node
3.  Worker compiles code using gcc
4.  Program executes with resource limits
5.  Output is sent back to user

------------------------------------------------------------------------

# 🧪 Operating System Concepts Used

### Process Management

fork(), exec(), waitpid(), signals

### Multithreading

pthread for parallel networking operations

### IPC

pipes for capturing program output

### Networking

UDP for discovery TCP for communication

### Resource Control

setrlimit for CPU and memory limits

------------------------------------------------------------------------

# 🛠️ Installation

Requirements: • Linux system • gcc • make

Build project:

``` bash
make clean && make all
```

This generates:

node → cluster daemon sender → client program

------------------------------------------------------------------------

# ▶️ Usage

### Start Node

``` bash
./node
```

Run multiple nodes on same network to form cluster.

### Submit Job

``` bash
./sender
```

Enter IP of any node.

Supported inputs: • Single C file • Folder containing multiple C files

Interactive programs are supported.

------------------------------------------------------------------------

# 🛡️ Security Rules

Programs attempting restricted system calls may be blocked.

Repeated violations may result in client restriction.

------------------------------------------------------------------------

# 📚 Learning Outcomes

This project demonstrates:

• Distributed system fundamentals • Practical socket programming •
Parallel execution techniques • Fault tolerant architecture • Real-time
communication pipelines

------------------------------------------------------------------------

# 👨‍💻 Contributors

Group 4 -- Operating Systems Mini Project
