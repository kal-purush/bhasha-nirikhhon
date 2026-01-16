import { NextApiRequest, NextApiResponse } from "next";
import { getServerSession } from "next-auth/next";
import prisma from "@/lib/prisma";
import { authOptions } from "@/apiAuth/[...nextauth]";

export default async function handler(
    req: NextApiRequest,
    res: NextApiResponse
) {
    const session = await getServerSession(req, res, authOptions); //authenticate user on the server side

    if (!session)
        return res
            .status(401)
            .json({ success: false, message: "Unauthorized because not logged in" });

    if (req.method === "GET") {
        try {
            const result = await prisma.user.findUnique({
                where: { id: session.user.id },
                include: {
                    patient: {
                        select: { insuranceId: true }
                    }
                }
            })
            return res.status(200).json({ success: true, data: result });
        }
        catch (error) {
            return res
                .status(500)
                .json({ success: false, message: "ERROR : Failed to retrieve data" });
        }
    }
}