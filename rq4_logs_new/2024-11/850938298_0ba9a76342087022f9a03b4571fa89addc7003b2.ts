import roomModel from "../models/RoomSchema";
import { Request, Response, NextFunction } from "express";

export async function getRoomDetails(
    request: Request,
    response: Response,
    next: NextFunction
) {
    const { roomId } = request.params
    try {
        console.log(roomId)
        const room = await roomModel.findOne({ roomId });
        if (!room) {
            throw new Error("Room not found");
        }
        response.status(200).json({
            roomId, 
            attemptStartedAt: room.createdAt.getTime(),
            userIdOne: room.participants[0],
            userIdTwo: room.participants[1],
            questionId: room.questionId,
        });
    } catch (error) {
        next(error);
    }
}