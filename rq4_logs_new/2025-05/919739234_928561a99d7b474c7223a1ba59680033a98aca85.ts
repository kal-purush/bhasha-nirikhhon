import PlayList from "@/entities/PlayList.entity";
import { EResponseMessage, EVisibility } from "@/types/enums";
import { NextFunction, Request, Response } from "express";
import mongoose from "mongoose";

export const createPlayList = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const { name, visibility } = req.body;

    if (!req.user?.id) {
      res.status(401).json({ message: EResponseMessage.INVALID_CREDENTIALS });
      return;
    }

    const ownerID = req.user.id;

    const newPlayList = await PlayList.create({
      _id: new mongoose.Types.ObjectId(),
      ownerID,
      name,
      visibility,
    });

    res.status(201).json(newPlayList);
  } catch (error) {
    next(error);
  }
};

export const getMyPlayList = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    if (!req.user?.id) {
      res.status(401).json({ message: EResponseMessage.INVALID_CREDENTIALS });
      return;
    }
    const ownerID = req.user.id;
    const playlists = await PlayList.find({ ownerID }).populate("song");

    res.status(200).json(playlists);
  } catch (error) {
    next(error);
  }
};

export const deletePlayList = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const playlistId = req.params.id;
    if (!req.user?.id) {
      res.status(401).json({ message: EResponseMessage.INVALID_CREDENTIALS });
      return;
    }
    const ownerID = req.user.id;

    const deleted = await PlayList.findOneAndDelete({
      _id: playlistId,
      ownerID,
    });

    if (!deleted) {
      res.status(404).json({ message: EResponseMessage.PLAYLIST_NOT_FOUND });
      return;
    }

    res.status(200).json(deleted._id);
  } catch (error) {
    next(error);
  }
};

export const updatePlayList = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const playlistId = req.params.id;

    if (!req.user?.id) {
      res.status(401).json({ message: EResponseMessage.INVALID_CREDENTIALS });
      return;
    }

    const ownerID = req.user.id;
    const { name, visibility, songs } = req.body;

    const updateData: Partial<{
      name: string;
      visibility: EVisibility;
      song: string[];
    }> = {};

    if (typeof name === "string" && name.trim() !== "") {
      updateData.name = name.trim();
    }

    if (Object.values(EVisibility).includes(visibility)) {
      updateData.visibility = visibility;
    }

    if (Array.isArray(songs)) {
      updateData.song = songs;
    }

    const updated = await PlayList.findOneAndUpdate(
      { _id: playlistId, ownerID },
      updateData,
      { new: true }
    ).populate("song");

    if (!updated) {
      res.status(404).json({ message: EResponseMessage.PLAYLIST_NOT_FOUND });
      return;
    }

    res.status(200).json(updated);
  } catch (error) {
    next(error);
  }
};