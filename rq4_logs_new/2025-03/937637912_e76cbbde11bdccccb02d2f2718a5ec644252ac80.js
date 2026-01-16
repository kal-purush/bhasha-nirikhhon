import GroupMessage from "../models/groupMessage.model.js";
import Group from "../models/group.model.js";
import { io, getReceiverSocketId } from "../lib/socket.js";
import cloudinary from "../lib/cloudinary.js";


export const sendGroupMessage = async (req, res) => {


    try {
        const { groupId } = req.params;
        const { text, image , } = req.body;
        const senderId = req.user._id;

        let imageUrl;
        if (image) {
            // Upload base64 image to cloudinary
            const uploadResponse = await cloudinary.uploader.upload(image);
            imageUrl = uploadResponse.secure_url;
        }
        // Check if group exists
        const group = await Group.findById(groupId);
        if (!group) return res.status(404).json({ message: "Group not found" });

        // Create new group message
        const newMessage = new GroupMessage({
            senderId,
            groupId,
            text,
            image: imageUrl,
        });

        await newMessage.save();


        if (groupId) {
            io.to(groupId).emit("receiveGroupMessage", newMessage);
        }

        res.status(201).json(newMessage);
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: "Error sending message" });
    }
};
export const getGroupMessages = async (req, res) => {
    const { groupId } = req.params;
    const myId = req.user._id;
    console.log(groupId);
    console.log(myId);
    console.log("Emitting group message to:", groupId);


    try {
        const messages = await GroupMessage.find({ groupId }).populate("senderId", "username profilePic").lean(); // Convert Mongoose objects to plain JSON

        // Convert senderId to a string for frontend compatibility
        messages.forEach((msg) => {
            msg.senderId = msg.senderId?._id.toString();  // Ensure it's a string
        });

        console.log(messages);

        res.status(200).json(messages);
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: "Error fetching messages" });
    }
};
