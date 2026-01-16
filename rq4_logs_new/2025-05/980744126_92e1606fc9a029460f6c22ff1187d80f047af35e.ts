"use server";
/* eslint-disable @typescript-eslint/no-explicit-any */
import { Chat } from "@/models/chat-model";

export const addToChat = async (payload: any) => {
  try {
    const chat = await Chat.create(payload);
    return JSON.parse(JSON.stringify(chat));
  } catch (error) {
    return {
      error,
    };
  }
};