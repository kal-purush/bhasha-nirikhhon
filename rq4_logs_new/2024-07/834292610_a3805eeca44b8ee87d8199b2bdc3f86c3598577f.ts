import { collection, addDoc, doc, serverTimestamp, getFirestore, query, orderBy, getDocs, where, updateDoc } from "firebase/firestore";
import firebase from "@/services/firebase-service";
import { Chat, Message } from "@/lib/types";

const db = getFirestore(firebase);

export async function createChat(userId: string): Promise<string> {
    const chatRef = await addDoc(collection(db, "chats"), {
        userId,
        createdAt: serverTimestamp(),
        isDeleted: false
    });
    return chatRef.id;
};

export async function findChatsByUserId(userId: string): Promise<Chat[]> {
    const chatsRef = collection(db, "chats");
    const userChatsQuery = query(
        chatsRef,
        where("userId", "==", userId),
        where("isDeleted", "==", false)
    );
    const querySnapshot = await getDocs(userChatsQuery);
    return querySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }) as Chat);
};

export async function deleteChat(chatId: string) {
    const chatRef = doc(db, "chats", chatId);
    await updateDoc(chatRef, {
        isDeleted: true
    });
};

export async function getMessages(chatId: string): Promise<Message[]> {
    const messagesRef = collection(db, "chats", chatId, "messages");
    const messagesQuery = query(messagesRef, orderBy("date", "asc"));
    const querySnapshot = await getDocs(messagesQuery);
    const data = querySnapshot.docs.map(doc => doc.data());
    
    return data.map((message: any) => {return {role: message.role, text: message.text, date: message.date.toDate()}});

};

export async function addMessage(chatId: string, userId: string, message: Message) {
    const messagesRef = collection(db, "chats", chatId, "messages");
    await addDoc(messagesRef, message);
};

