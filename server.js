const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static("public"));

/*
 * Tracks active sender child-processes, keyed by socket.id.
 * Each entry: { proc: ChildProcess, phase: "secret"|"ip"|"ready"|"running" }
 */
const activeJobs = new Map();

/*
 * dispatchToGrid
 * ──────────────
 * Spawns ./sender, walks through its three-step stdin handshake
 * (secret → IP → path/command), then streams all output back to
 * the browser via Socket.IO.
 */
function dispatchToGrid(targetPath, socketId, clusterSecret, nodeIp = "127.0.0.1", forceLocal = false) {
  const socket = io.sockets.sockets.get(socketId);
  if (!socket) return;

  const existing = activeJobs.get(socketId);
  if (existing && !existing.proc.killed) {
    existing.proc.kill();
  }

  const sender = spawn("./sender");
  activeJobs.set(socketId, { proc: sender, phase: "secret" });

  let outputBuffer = "";

  function handleOutput(data) {
    const text = data.toString();
    outputBuffer += text;

    const job = activeJobs.get(socketId);
    if (!job) return;

    if (job.phase === "secret") {
      if (outputBuffer.includes("Cluster Secret:")) {
        sender.stdin.write(clusterSecret + "\n");
        job.phase = "ip";
        
        // Push whatever banner text came *before* the prompt to the UI
        let beforeStr = outputBuffer.substring(0, outputBuffer.indexOf("Cluster Secret:"));
        outputBuffer = ""; // Reset buffer
        
        const clean = beforeStr.replace(/\x1b\[[0-9;]*[mGKHF]/g, "");
        if (clean.trim()) socket.emit("terminal_output", clean);
        return;
      }
    } else if (job.phase === "ip") {
      if (outputBuffer.includes("Enter target node IP")) {
        sender.stdin.write(nodeIp + "\n");
        job.phase = "ready";
        
        // Push whatever text came *before* the IP prompt
        let beforeStr = outputBuffer.substring(0, outputBuffer.indexOf("Enter target node IP"));
        outputBuffer = "";
        
        const clean = beforeStr.replace(/\x1b\[[0-9;]*[mGKHF]/g, "");
        if (clean.trim()) socket.emit("terminal_output", clean);
        socket.emit("terminal_output", `[WEB] Connecting to grid node ${nodeIp}...\n`);
        return;
      }
    } else if (job.phase === "ready") {
      if (outputBuffer.includes("CODE>")) {
        const cmd = forceLocal ? `local ${targetPath}` : targetPath;
        sender.stdin.write(cmd + "\n");
        job.phase = "running";
        
        let beforeStr = outputBuffer.substring(0, outputBuffer.indexOf("CODE>"));
        outputBuffer = "";
        
        const clean = beforeStr.replace(/\x1b\[[0-9;]*[mGKHF]/g, "");
        if (clean.trim()) socket.emit("terminal_output", clean);
        return;
      }
    } else if (job.phase === "running") {
        // We are fully connected, just stream everything
        const clean = text.replace(/\x1b\[[0-9;]*[mGKHF]/g, "");
        if (clean.trim()) socket.emit("terminal_output", clean);
    }
  }

  sender.stdout.on("data", handleOutput);
  sender.stderr.on("data", handleOutput);

  sender.on("close", (code) => {
    socket.emit(
      "terminal_output",
      `\n[System] Dispatcher disconnected (exit ${code})\n`
    );
    activeJobs.delete(socketId);
  });

  sender.on("error", (err) => {
    socket.emit("terminal_output", `\n[System] Failed to start sender: ${err.message}\n`);
    activeJobs.delete(socketId);
  });
}

// ─── REST Endpoints ──────────────────────────────────────────────────────────

app.post("/api/run-code", (req, res) => {
  const { code, socketId, clusterSecret, nodeIp, forceLocal } = req.body;

  if (!clusterSecret) {
    return res.status(400).json({ error: "Cluster secret is required." });
  }
  if (!code || !code.trim()) {
    return res.status(400).json({ error: "No code provided." });
  }

  const tempPath = path.join("/tmp", `grid_web_${Date.now()}_${socketId.slice(0,6)}.c`);
  try {
    fs.writeFileSync(tempPath, code);
  } catch (e) {
    return res.status(500).json({ error: "Failed to write temp file." });
  }

  dispatchToGrid(tempPath, socketId, clusterSecret, nodeIp || "127.0.0.1", !!forceLocal);
  res.json({ success: true });
});

app.post("/api/run-path", (req, res) => {
  const { targetPath, socketId, clusterSecret, nodeIp, forceLocal } = req.body;

  if (!clusterSecret) {
    return res.status(400).json({ error: "Cluster secret is required." });
  }

  dispatchToGrid(targetPath, socketId, clusterSecret, nodeIp || "127.0.0.1", !!forceLocal);
  res.json({ success: true });
});

// ─── Socket.IO ───────────────────────────────────────────────────────────────

io.on("connection", (socket) => {
  socket.on("terminal_input", (data) => {
    const job = activeJobs.get(socket.id);
    if (job && !job.proc.killed) {
      job.proc.stdin.write(data + "\n");
    }
  });

  socket.on("disconnect", () => {
    const job = activeJobs.get(socket.id);
    if (job && !job.proc.killed) job.proc.kill();
    activeJobs.delete(socket.id);
  });
});

// ─── Start ───────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n[Web UI] Running on http://localhost:${PORT}`);
  console.log(`[Web UI] Make sure ./sender is compiled and in the same directory.\n`);
});