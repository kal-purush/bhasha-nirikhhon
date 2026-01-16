import { Request, Response, NextFunction } from "express";
import { RoomController } from "../../controllers/room";

export const getAllRooms = async (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  try {
    const { serverId } = req.params;
    const rooms = await RoomController.getAll(Number(serverId));
    res.json({
      message: "All rooms",
      rooms,
    });
  } catch (error) {
    next(error);
  }
};

export const createRoom = async (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  try {
    const { serverId } = req.params;
    const { name } = req.body;
    const newRoom = await RoomController.create(Number(serverId), name);
    res.status(201).json({
      message: "New room",
      room: newRoom,
    });
  } catch (error) {
    next(error);
  }
};

export const deleteRoom = async (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  try {
    const { roomId, serverId } = req.params;
    await RoomController.delete(Number(serverId), Number(roomId));
    res.status(204).json();
  } catch (error) {
    next(error);
  }
};