import { FriendRequest } from "@/models/FriendRequest";
import { db } from "@/utils/firebaseConfig";
import { collection, addDoc } from "firebase/firestore";

async function sendFriendReqeust(sender: string, receiver: string) {
  const friendRequest: FriendRequest = {
    id: "", // firebase will generate this
    sender,
    receiver,
    status: "pending",
    createdAt: new Date().toISOString(),
  };

  try {
    const docRef = await addDoc(
      collection(db, "FriendRequests"),
      friendRequest
    );
    console.log(`friend request send with id: ${docRef.id}`);
  } catch (error) {
    console.error(`error sending friend request: ${error}`);
  }
}