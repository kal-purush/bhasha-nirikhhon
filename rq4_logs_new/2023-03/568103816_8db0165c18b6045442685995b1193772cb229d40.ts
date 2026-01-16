import { NextApiRequest, NextApiResponse } from 'next';
import { getNodeMailerTransporter } from '../../../helper/nodemailer';
import prisma from '../../../lib/prisma';
import { getEmailTemplate } from '../../../helper/mailTemplaes';

// this cron job does:
// - declines all pending requests where timelimit is over
// - sends email to host that timelimit is over
// - sends email to guests that request was declined
export default async function handler(
    req: NextApiRequest,
    res: NextApiResponse
) {
    if (req.method === 'POST') {
        try {
            const { authorization } = req.headers;

            if (authorization === `Bearer ${process.env.API_SECRET_KEY}`) {
                const now = new Date();
                const oneHourAgo = new Date(now);
                oneHourAgo.setHours(oneHourAgo.getHours() - 1);
                const transporter = getNodeMailerTransporter();

                // get all events where timelimit was last hour and event was not cancelled
                const events = await prisma.event.findMany({
                    where: {
                        AND: [
                            { timeLimit: { gte: oneHourAgo, lte: now } },
                            { NOT: { status: 'CANCELLED' } },
                        ],
                    },
                    include: {
                        requests: {
                            include: {
                                User: true,
                            },
                        },
                        host: true,
                    },
                });

                // decline all pending requests
                const upateRequests = await prisma.request.updateMany({
                    where: {
                        AND: [
                            {
                                Event: {
                                    AND: [
                                        {
                                            timeLimit: {
                                                gte: oneHourAgo,
                                                lte: now,
                                            },
                                        },
                                        { NOT: { status: 'CANCELLED' } },
                                    ],
                                },
                            },
                            { status: 'PENDING' },
                        ],
                    },
                    data: {
                        status: 'DECLINED',
                    },
                });

                events.forEach((event) => {
                    // mail to host
                    const mailData = getEmailTemplate({
                        hostFirstName: event.host.firstName,
                        eventTitle: event.title,
                        type: 'timelimit-host',
                        eventDetail: {
                            amountOfGuests: event.currentParticipants,
                        },
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

                    // mail to each guest
                    event.requests.forEach((request) => {
                        if (request.status === 'PENDING') {
                            const mailData = getEmailTemplate({
                                hostFirstName: event.host.firstName,
                                eventTitle: event.title,
                                guestName: request.User.firstName,
                                type: 'declined',
                            });

                            const mail = {
                                from: 'studentenfuttermmp3@gmail.com',
                                to: request.User.email,
                                ...mailData,
                            };
                            transporter.sendMail(mail, function (err, info) {
                                if (err) {
                                    console.log(err);
                                } else {
                                    console.log(info);
                                }
                            });
                        }
                    });
                });
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