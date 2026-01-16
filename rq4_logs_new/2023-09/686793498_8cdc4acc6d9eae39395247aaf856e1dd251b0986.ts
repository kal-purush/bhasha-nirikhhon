import SessionUtil from "@src/util/SessionUtil";
import { ISessionUser, PrismaClient } from "hive-link-common";
import { Request, Response } from "express";
import HttpStatusCodes from "@src/constants/HttpStatusCodes";

const prisma = new PrismaClient();

export const RoomsController = {
    getAll: async function getAll(_: Request, res: Response) {
        const sUser = await SessionUtil.getSessionData<ISessionUser>(_)
          .then((usr) => usr as ISessionUser)
          .catch((err) => {
            throw err;
          });
        const usrHome = await prisma.user_home.findUnique({
            where: { id: parseInt(_.params.home_id) },
            include: { user_room: true },
            });
        return res.status(HttpStatusCodes.OK).json({ rooms: usrHome?.user_room });
            }
}