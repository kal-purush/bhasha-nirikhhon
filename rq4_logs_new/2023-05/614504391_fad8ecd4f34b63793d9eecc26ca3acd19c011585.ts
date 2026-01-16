import { NextApiRequest, NextApiResponse } from "next";
import prisma from "@/lib/prisma";
import { Role } from "@prisma/client";

export default async function handler(
    req: NextApiRequest,
    res: NextApiResponse
) {
    if (req.method === "GET") {
        try {
            const role = req.query.role;

            if (role === Role.PATIENT) {
                const patients = await prisma.patient.findMany(
                    {
                        include: {
                            user: {
                                select: {
                                    firstName: true,
                                    lastName: true,
                                    email: true,
                                    id: true,
                                },
                            }
                        }
                    }
                );
                return res.status(200).json({ success: true, data: patients });
            }
            else if (role === Role.DOCTOR) {
                const doctors = await prisma.doctor.findMany(
                    {
                        include: {
                            user: {
                                select: {
                                    firstName: true,
                                    lastName: true,
                                    email: true,
                                    id: true,
                                },
                            }
                        }
                    }
                );
                return res.status(200).json({ success: true, data: doctors });
            }
            else if (role === Role.RECEPTIONIST) {
                const receptionists = await prisma.receptionist.findMany(
                    {
                        include: {
                            user: {
                                select: {
                                    firstName: true,
                                    lastName: true,
                                    email: true,
                                    id: true,
                                },
                            }
                        }
                    }
                );
                return res.status(200).json({ success: true, data: receptionists });
            }
            else if (role === Role.LAB_ASSISTANT) {
                const labAssistants = await prisma.labAssistant.findMany(
                    {
                        include: {
                            user: {
                                select: {
                                    firstName: true,
                                    lastName: true,
                                    email: true,
                                    id: true,
                                },
                            }
                        }
                    });
                return res.status(200).json({ success: true, data: labAssistants });

            }
            else if (role === Role.LAB_SUPERVISOR) {
                const labSupervisors = await prisma.labSupervisor.findMany(
                    {
                        include: {
                            user: {
                                select: {
                                    firstName: true,
                                    lastName: true,
                                    email: true,
                                    id: true,
                                },
                            }
                        }
                    }
                );
                return res.status(200).json({ success: true, data: labSupervisors });
            }


        } catch (error) {
            return res
                .status(500)
                .json({ success: false, message: "Failed to retrieve patients" });
        }
    }

    return res
        .status(400)
        .json({ success: false, message: "Invalid request method" });
}