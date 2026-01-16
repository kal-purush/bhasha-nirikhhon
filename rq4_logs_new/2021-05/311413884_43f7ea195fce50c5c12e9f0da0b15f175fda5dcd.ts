import { Request, NextFunction, Response } from "express";
import { ServerController } from "../../controllers/server";
import ErrorHandler from "../../lib/error-handler";

export const getAllServers = async (
  _req: Request,
  res: Response,
  next: NextFunction,
) => {
  try {
    const allServers = await ServerController.getAll();
    return res.json({
      servers: allServers,
    });
  } catch (error) {
    next(error);
  }
};

export const createServer = async (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  const { name, icon } = req.body;
  const { id } = req.user as any;
  try {
    const newServer = await ServerController.create(id, name, icon);
    res.status(201).json({
      server: newServer,
    });
  } catch (error) {
    if (error.errors?.length > 0) {
      return next(new ErrorHandler(400, error.message, error.errors));
    }
    next(error);
  }
};

export const getOneServer = async (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  try {
    const { serverId } = req.params;
    const server = await ServerController.getOne(Number(serverId));
    if (!server) {
      return next(new ErrorHandler(404, "No server found"));
    }
    return res.json({
      server,
    });
  } catch (error) {
    next(error);
  }
};

export const updateServer = async (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  try {
    const { serverId } = req.params;
    await ServerController.update(Number(serverId), req.body);
    res.status(200).json({});
  } catch (error) {
    next(error);
  }
};

export const deleteServer = async (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  try {
    const { serverId } = req.params;
    await ServerController.delete(Number(serverId));
    return res.status(204).json({});
  } catch (error) {
    next(error);
  }
};