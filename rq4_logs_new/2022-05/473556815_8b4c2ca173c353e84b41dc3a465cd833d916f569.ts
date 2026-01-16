import * as functions from "firebase-functions";
import { MessageData, Updates } from "../../assets/Types";
import { ExpoPushClient } from "../clients/ExpoPushClient";
import { ChatRepository } from "../repository/ChatRepository";
import { UserRepository } from "../repository/UserRepository";

export class chatService {
  private chat_repository: ChatRepository;
  private user_repository: UserRepository;
  private expoPushClient: ExpoPushClient;

  constructor(user_repo: UserRepository, chat_repo: ChatRepository) {
    this.chat_repository = chat_repo;
    this.user_repository = user_repo;
    this.expoPushClient = new ExpoPushClient();
  }

  async onMessageCreate(
    snapshot: functions.database.DataSnapshot,
    context: functions.EventContext
  ) {
    const chatId: string = context.params.chatId;
    const messageId = context.params.messageId;
    functions.logger.info("New message in chat " + chatId + ": " + messageId);

    const messageData = snapshot.val();
    const text: string = messageData.text;
    const senderId: string = messageData.user._id;
    const senderName: string = messageData.user.name;

    //send push notification to all members of chat
    let message: MessageData = {
      title: "Roomeight",
      body: senderName + ": " + text,
    };
    const members = await this.chat_repository.getMembers(chatId);
    functions.logger.debug("Members: " + members);
    const recepients = members.filter((member) => member !== senderId);
    let expoPushTokens: string[] = [];

    recepients.forEach(async (recepient) => {
      const profile = await this.user_repository.getProfileById(recepient);
      functions.logger.debug("Profile: " + profile);
      const devicePushTokens: string[] = profile.devicePushTokens;
      expoPushTokens.push(...devicePushTokens);
    });
    functions.logger.debug("ExpoPushTokens: " + expoPushTokens);
    this.expoPushClient.pushToClients(expoPushTokens, message);

    //update last sender and last message in chatinfo
    let updates: Updates = {};
    updates["/lastSender"] = senderName;
    updates["/lastMessage"] = text;
    updates["/timestamp"] = messageData.timestamp;
    await this.chat_repository.updateChatInfo(chatId, updates);
  }
}