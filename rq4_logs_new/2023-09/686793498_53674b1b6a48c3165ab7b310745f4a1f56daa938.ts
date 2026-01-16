import SessionUtil from "@src/util/SessionUtil";
import { Request, Response } from "express";
import { ISessionUser, PrismaClient } from "hive-link-common";

const prisma = new PrismaClient();

export const UserDevicesController = {
  getAll: async (req: Request, res: Response) => {
    const sUser = await SessionUtil.getSessionData<ISessionUser>(req)
      .then((usr) => usr as ISessionUser)
      .catch((err) => {
        throw err;
      });
    if (!sUser) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const home = await prisma.user_home.findUnique({
      where: { id: parseInt(req.params.userHomeId) },
      include: {
        user_device: {
          include: { device: {
            include: { device_category: true }
          }, product: true },
        },
      },
    });
    if (!home) {
      return res.status(404).json({ error: "Home not found" });
    }
    return res.status(200).json({ devices: home.user_device });
  },
};