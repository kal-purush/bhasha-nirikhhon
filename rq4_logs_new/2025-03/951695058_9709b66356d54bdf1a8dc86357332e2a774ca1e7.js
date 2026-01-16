import { Conversation } from "../model/conversationModel.js";
import { Message } from "../model/messageModel.js";

export const sendMessage = async (req, res) => {
  try {
    const senderId = req.id;
    const recieverId = req.params.id;
    const { message } = req.body;

    let conversation = await Conversation.findOne({
      participants: { $all: [senderId, recieverId] },
    });

    // Establish the conversation if it doesn't exist
    if (!conversation) {
      conversation = await Conversation.create({
        participants: [senderId, recieverId],
      });
    }

    const newMessage = await Message.create({
      senderId,
      recieverId,
      message,
    });

    if (newMessage) {
      conversation.messages.push(newMessage._id);
      await Promise.all([newMessage.save(), conversation.save()]);

      // implement socket.io here for real time data transfer

      return res
        .status(200)
        .json({ message: "Message sent", success: true, data: newMessage });
    }
  } catch (error) {
    res.status(500).json({ message: error.message, success: false });
  }
};

export const getMessages = async (req, res) => {
  try {
    const senderId = req.id;
    const recieverId = req.params.id;
    const conversation = await Conversation.find({
      participants: { $all: [senderId, recieverId] },
    });

    if (!conversation) {
      return res.status(200).json({ messages: [], success: true });
    }

    return res
      .status(200)
      .json({ messages: conversation?.messages, success: true });
  } catch (error) {
    res.status(500).json({ message: error.message, success: false });
  }
};