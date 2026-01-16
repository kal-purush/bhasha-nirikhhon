import { NextApiRequest, NextApiResponse } from 'next';
import { getNodeMailerTransporter } from '../../../helper/nodemailer';
import prisma from '../../../lib/prisma';
import { getEmailTemplate } from '../../../helper/mailTemplaes';

// this cron job does:
// - set event status to over on upcoming events
// - sends email to host that he should upload an image to event
// - creates a notification for host to upload an image to event
// - TODO: review email (later)

export default async function handler(
    req: NextApiRequest,
    res: NextApiResponse
) {
    if (req.method === 'POST') {
        try {
            const { authorization } = req.headers;

            if (authorization === `Bearer ${process.env.API_SECRET_KEY}`) {
                const yesterdayBeginning = new Date();
                yesterdayBeginning.setDate(yesterdayBeginning.getDate() - 1);
                yesterdayBeginning.setHours(0, 0, 0, 0);

                const yesterdayEnding = new Date(yesterdayBeginning);
                yesterdayEnding.setDate(yesterdayEnding.getDate() + 1);

                const transporter = getNodeMailerTransporter();

                // get all events that took place yesterday
                const events = await prisma.event.findMany({
                    where: {
                        AND: [
                            {
                                date: {
                                    gte: yesterdayBeginning,
                                    lte: yesterdayEnding,
                                },
                            },
                            { NOT: { status: 'CANCELLED' } },
                        ],
                    },
                    include: {
                        host: { select: { firstName: true, email: true } },
                    },
                });

                await prisma.event.updateMany({
                    where: {
                        AND: [
                            {
                                date: {
                                    gte: yesterdayBeginning,
                                    lte: yesterdayEnding,
                                },
                            },
                            { NOT: { status: 'CANCELLED' } },
                        ],
                    },
                    data: {
                        status: 'OVER',
                    },
                });

                for (const event of events) {
                    // mail to host
                    const mailData = getEmailTemplate({
                        hostFirstName: event.host.firstName,
                        eventTitle: event.title,
                        eventId: event.id,
                        type: 'imageUpload',
                    });

                    const mail = {
                        from: 'studentenfuttermmp3@gmail.com',
                        to: event.host.email,
                        ...mailData,
                    };
                    transporter.sendMail(mail, function (err, info) {
                        if (err) {
                            console.log(err);
                        } else {
                            console.log(info);
                        }
                    });

                    //notification for host
                    await prisma.notification.create({
                        data: {
                            Event: { connect: { id: event.id } },
                            User: { connect: { id: event.authorId } },
                            type: 'EVENT',
                            message: 'Add a photo now!',
                        },
                    });
                }
                res.status(200).json({ statusCode: 200, success: true });
            } else {
                res.status(401).json({ statusCode: 401, success: false });
            }
        } catch (err) {
            res.status(500).json({ statusCode: 500, message: err.message });
        }
    } else {
        res.setHeader('Allow', 'POST');
        res.status(405).end('Method Not Allowed');
    }
}