import express from "express";
import { createServer } from "http";
import { WebSocket, WebSocketServer } from "ws";
import dotenv from "dotenv";
import QueueHandler from "./worker/queueHandler";
import FriendDataHandler from "./worker/frinedsDataHandler";
import CacheHandler from "./worker/cacheHandler";
import { LocationData } from "./types/locationData";

dotenv.config();

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

const queueHandler = new QueueHandler();
const friendDataHandler = new FriendDataHandler();
const cacheHandler = new CacheHandler();

const LOCATION_UPDATE_INTERVAL = 1000;
const PORT = 3000;

const clientsMap: Map<string, WebSocket> = new Map();

app.use(express.json());

wss.on("connection", (ws) => {
  console.log("A user connected");

  ws.on("message", async (message) => {
    try {
      const data: LocationData = JSON.parse(message.toString());
      const { student_id, latitude, longitude } = data;

      clientsMap.set(student_id, ws);

      await cacheHandler.setLocation(student_id, { student_id, latitude, longitude }, 600);

      const friends = await friendDataHandler.getFriends(student_id);
      const friendsIds = friends.map((friend) => friend.student_id);
      console.log(`hello ${friendsIds}`);
      

      if(friends.length === 0){
        await queueHandler.enqueueLocationUpdate(student_id, latitude, longitude, []);
      }else{
        const friendsLocations = await cacheHandler.getFriendsLocations(friendsIds);
        console.log(`Friends' location data: ${JSON.stringify(friendsLocations)}`);

        await queueHandler.enqueueLocationUpdate(student_id, latitude, longitude, friendsLocations);
        console.log("Location update added to the queue");
      }
    } catch (error) {
      console.error("Error handling location update:", error);
    }
  });

  ws.on("close", async () => {
    let disconnectedStudentId: string | undefined;
    clientsMap.forEach((client, student_id) => {
      if (client === ws) {
        disconnectedStudentId = student_id;
      }
    });

    if (disconnectedStudentId) {
      await cacheHandler.removeLocation(disconnectedStudentId);
      clientsMap.delete(disconnectedStudentId);
      console.log(`Removed client with student_id: ${disconnectedStudentId}`);
    }
  });
});

setInterval(async () => {
  try {
    const queueSize = await queueHandler.getQueueSize();
    if (queueSize > 0) {
      console.log(`Queue size: ${queueSize} items waiting for processing`);

      const batchSize = 10; 
      await queueHandler.processQueueBatch(batchSize, clientsMap);
    } else {
      console.log("No location updates in the queue");
    }
  } catch (error) {
    console.error("Error processing location updates:", error);
  }
}, LOCATION_UPDATE_INTERVAL);

server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});