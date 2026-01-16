import { NextApiRequest, NextApiResponse } from "next";
import { getServerSession } from "next-auth/next";
import prisma from "@/lib/prisma";
import { authOptions } from "@/apiAuth/[...nextauth]";
import { Role, Status } from "@prisma/client";


export default async function handler(
    req: NextApiRequest,
    res: NextApiResponse
) {
    const session = await getServerSession(req, res, authOptions); //authenticate user on the server side

    // let accessGranted = false;

    if (!session)
        return res
            .status(401)
            .json({ success: false, message: "Unauthorized because not logged in" });
    const { visit_id } = req.query
    if (req.method == "GET") {
        if (session.user?.role == Role.PATIENT) {
            try {
                const visit = await prisma.visit.findUnique({
                    where: {
                        visitId: visit_id.toString(),
                    },
                })
                if (visit.patientId == session.user?.id) {
                    return res.status(200).json({ success: true, data: visit });
                }
                else {
                    return res.status(401).json({ success: false, message: "You can't see this patient's data" });
                }
            }
            catch (error) {
                //here should be a redirect to a general purpose error page
                return res
                    .status(500)
                    .json({ success: false, message: "ERROR : Failed to retrieve data" });
            }
        }

        if (session.user?.role == Role.RECEPTIONIST) {
            try {
                const visit = await prisma.visit.findUnique({
                    where: {
                        visitId: visit_id.toString(),
                    },
                })
                return res.status(200).json({ success: true, data: visit });
            }
            catch (error) {
                //here should be a redirect to a general purpose error page
                return res
                    .status(500)
                    .json({ success: false, message: "ERROR : Failed to retrieve data" });
            }
        }


        //return not authorized
        return res
            .status(401)
            .json({ success: false, message: "you are not an authorized person" });
    }

    if (req.method == "PUT") {
        if (session.user?.role == Role.PATIENT) {
            try {
                const visit = await prisma.visit.findUnique({
                    where: {
                        visitId: visit_id.toString(),
                    },
                })
                if (visit.patientId == session.user?.id) {
                    let dataClause = {}
                    if (req.body.status) {
                        dataClause = {
                            status: Status.CANCELLED,
                        }
                    }
                    if (req.body.date) {
                        dataClause = {
                            date: req.body.date,
                        }
                    }

                    const results = await prisma.visit.update({
                        where: {
                            visitId: visit_id.toString(),
                        },
                        data: dataClause,
                    })
                    return res.status(200).json({ success: true, data: results });
                }
                else {
                    return res.status(401).json({ success: false, message: "You can't see this patient's data" });
                }
            }
            catch (error) {
                //here should be a redirect to a general purpose error page
                return res
                    .status(500)
                    .json({ success: false, message: "ERROR : Failed to retrieve data" });
            }
        }

        if (session.user?.role == Role.RECEPTIONIST) {
            try {
                let dataClause = {}
                if (req.body.status) {
                    dataClause = {
                        status: Status.CANCELLED,
                    }
                }
                else {
                    if (req.body.date) {
                        dataClause = {
                            date: req.body.date,
                        }
                    }
                    if (req.body.doctorId) {
                        dataClause = {
                            ...dataClause,
                            doctorId: req.body.doctorId,
                        }
                    }
                }
                const results = await prisma.visit.update({
                    where: {
                        visitId: visit_id.toString(),
                    },
                    data: {
                        ...dataClause,
                    }
                })
                return res.status(200).json({ success: true, data: results });
            }
            catch (error) {
                //here should be a redirect to a general purpose error page
                return res
                    .status(500)
                    .json({ success: false, message: "ERROR : Failed to retrieve data" });
            }
        }


        //return not authorized
        return res
            .status(401)
            .json({ success: false, message: "you are not an authorized person" });
    }
    return res
        .status(400)
        .json({ success: false, message: "Invalid request method" });
}