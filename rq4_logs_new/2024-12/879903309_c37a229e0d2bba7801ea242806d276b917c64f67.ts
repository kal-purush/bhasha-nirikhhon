import express from "express";
import { createServer } from "http";
import { Server, Socket } from "socket.io";
import cors from "cors";

// Define message interface
interface Message {
  text: string;
  timestamp: string;
  type: "incoming" | "outgoing";
  manual: string;
}

// Initialize express app
const app = express();
app.use(cors());

// Create HTTP server
const httpServer = createServer(app);

// Initialize Socket.IO with CORS configuration
const io = new Server(httpServer, {
  cors: {
    origin: "http://localhost:3000", // Your Next.js app URL
    methods: ["GET", "POST"],
    credentials: true,
  },
  // Add any additional Socket.IO configurations here
  pingTimeout: 60000,
  pingInterval: 25000,
});

// Track connected clients count
let connectedClients = 0;
const sleep = (ms: number): Promise<void> => {
  return new Promise((resolve) => setTimeout(resolve, ms));
};

// Socket.IO connection handling
io.on("connection", (socket: Socket) => {
  connectedClients++;
  console.log(`New client connected. Total clients: ${connectedClients}`);

  // Send welcome message
  //   const welcomeMessage: Message = {
  //     text: "Welcome to the Manual Reader Chat Bot!",
  //     timestamp: new Date().toISOString(),
  //     type: "incoming",
  //   };

  //   socket.emit("message", welcomeMessage);

  // Handle incoming messages
  socket.on("message", async (message: Message) => {
    try {
      console.log("Received:", message.text);

      // Create the response message
      const response: Message = {
        text: `received: ${message.manual} + ", " + ${message.text}`,
        timestamp: new Date().toISOString(),
        type: "incoming",
        manual: message.manual,
      };

      // Send the response back
      await sleep(2000);
      socket.emit("message", response);
    } catch (error) {
      console.error("Error processing message:", error);

      // Send error message back to client
      const errorMessage: Message = {
        text: "Error processing your message",
        timestamp: new Date().toISOString(),
        manual: "error",
        type: "incoming",
      };
      socket.emit("message", errorMessage);
    }
  });

  // Handle client disconnection
  socket.on("disconnect", (reason) => {
    connectedClients--;
    console.log(`Client disconnected. Reason: ${reason}`);
    console.log(`Remaining clients: ${connectedClients}`);
  });

  // Handle errors
  socket.on("error", (error) => {
    console.error("Socket error:", error);
  });
});

// Start server
const PORT = process.env.PORT || 8080;
httpServer.listen(PORT, () => {
  console.log(`Socket.IO server is running on http://localhost:${PORT}`);
});

// Handle server shutdown gracefully
process.on("SIGINT", () => handleServerShutdown());
process.on("SIGTERM", () => handleServerShutdown());

function handleServerShutdown() {
  console.log("\nShutting down Socket.IO server...");

  // Close all connections
  io.close(() => {
    console.log("Server closed successfully");
    process.exit(0);
  });
}