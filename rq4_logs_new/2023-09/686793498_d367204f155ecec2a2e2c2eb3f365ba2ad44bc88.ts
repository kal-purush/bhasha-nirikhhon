import HttpStatusCodes from "@src/constants/HttpStatusCodes";
import SessionUtil from "@src/util/SessionUtil";
import { Request, Response } from "express"; // Import Request and Response from Express
import { ISessionUser, PrismaClient } from "hive-link-common";

// ...
const prisma = new PrismaClient();
// Update the getAll function with explicit types for request and response
async function getAll(_: Request, res: Response) {
    const sUser = await SessionUtil.getSessionData<ISessionUser>(_).then((usr) => usr as ISessionUser);
    const homes = await prisma.user.findUnique({ where: { id: sUser.id}, include: { user_home: true } });
    return res.status(HttpStatusCodes.OK).json({ homes });
}

export default { getAll } as const;