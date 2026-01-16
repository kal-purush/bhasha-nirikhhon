import { Response, Request, NextFunction } from "express";
import { MemberController } from "../../controllers/member";

export const getAllMembers = async (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  try {
    const { serverId } = req.params;
    const members = await MemberController.getAll(Number(serverId));
    res.json({
      members,
    });
  } catch (error) {
    next(error);
  }
};

export const getOneMember = async (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  const { userId, serverId } = req.params;
  try {
    const member = await MemberController.getOne(
      Number(serverId),
      Number(userId)
    );
    res.json({
      member,
    });
  } catch (error) {
    next(error);
  }
};

export const joinServer = async (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  try {
    const { serverId } = req.params;
    const { inviteCode } = req.body;
    const { id } = req.user as any;
    const newMember = await MemberController.create(
      Number(serverId),
      id,
      inviteCode
    );
    res.status(201).json({
      member: newMember,
    });
  } catch (error) {
    next(error);
  }
};

export const deleteMember = async (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  try {
    // TODO: only the owner should be able to delete member.
    const { serverId } = req.params;
    const { userId } = req.body;
    await MemberController.delete(Number(serverId), userId);
    res.status(204).json({});
  } catch (error) {
    next(error);
  }
};