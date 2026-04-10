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

// This map tracks active sender processes
const activeJobs = new Map();

function dispatchToGrid(targetPath, socketId) {
  const socket = io.sockets.sockets.get(socketId);
  if (!socket) return;

  // Spawn your existing C sender process
  const sender = spawn("./sender");
  activeJobs.set(socketId, sender);

  // 1. Send the default local IP to the sender prompt
  sender.stdin.write(`127.0.0.1\n`);

  // 2. Wait 500ms for grid connection, then send the file/folder path
  setTimeout(() => {
    sender.stdin.write(`${targetPath}\n`);
  }, 500);

  // 3. Stream stdout (normal messages) to the web terminal
  sender.stdout.on("data", (data) => {
    socket.emit("terminal_output", data.toString());
  });

  // 4. Stream stderr (errors) to the web terminal
  sender.stderr.on("data", (data) => {
    socket.emit("terminal_output", data.toString());
  });

  sender.on("close", (code) => {
    socket.emit(
      "terminal_output",
      `\n[System] Dispatcher disconnected (Code: ${code})\n`,
    );
    activeJobs.delete(socketId);
  });
}

// Endpoint 1: Handle raw code typed in the browser
app.post("/api/run-code", (req, res) => {
  const { code, socketId } = req.body;
  // Save web code to a temporary C file
  const tempPath = path.join("/tmp", `grid_web_${Date.now()}.c`);
  fs.writeFileSync(tempPath, code);

  dispatchToGrid(tempPath, socketId);
  res.json({ success: true });
});

// Endpoint 2: Handle absolute paths to files or folders
app.post("/api/run-path", (req, res) => {
  const { targetPath, socketId } = req.body;
  dispatchToGrid(targetPath, socketId);
  res.json({ success: true });
});

// Clean up processes on disconnect
// Listen for connections and handle terminal inputs
io.on("connection", (socket) => {
  // NEW: Listen for user input from the web terminal
  socket.on("terminal_input", (data) => {
    const process = activeJobs.get(socket.id);
    // If a job is running, pipe the text directly into the C executable
    if (process && !process.killed) {
      process.stdin.write(data + "\n");
    }
  });

  // Clean up processes on disconnect
  socket.on("disconnect", () => {
    const process = activeJobs.get(socket.id);
    if (process) process.kill();
  });
});

const PORT = 3000;
server.listen(PORT, () => {
  console.log(`\n[Web UI] Running on http://localhost:${PORT}`);
});
