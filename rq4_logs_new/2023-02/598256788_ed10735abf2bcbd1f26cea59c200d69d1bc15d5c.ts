import { Socket, Server } from 'socket.io';
import { Router, Request, Response } from 'express'
import 'dotenv/config';
import verifyToken, { verifyTokenSocketio } from '../middlewares/verifyToken';

import prismaClient from '../services/prisma'
import prisma from '../services/prisma';

interface Chat {
    messages: {
        id:string,
        authorId:string,
        content: string,
        createdAt: string
    }[],
    id: String,
    lastMessageTime: String,
    user: {
        username: String,
        imageUrl: string,
        online: boolean,
        id: string,
    }
}

interface MessageData {
    auth?: {
        token?: string
    },
    data?: {
        content: string,
        toId: string
    }

}

export class ChatsController {

    router = Router()
    constructor(globalSocket: Server) {
        this.router.get('/chats', verifyToken, (request: Request, response: Response) => { this.read(request, response, globalSocket) })
    }

    async onSocketConnection(userSocket: Socket) {  
        

        const tokenVerifyResults = verifyTokenSocketio(userSocket.handshake.auth.token);
        if (tokenVerifyResults === "invalid" || !tokenVerifyResults) {
            userSocket.emit('unauthenticated');
            return userSocket.disconnect();
        };

        const userAccount = await prismaClient.user.findFirst({
            where: {
                id: tokenVerifyResults
            }
        })
        if (!userAccount) {
            userSocket.emit('unauthenticated');
            return userSocket.disconnect();
        }
        userSocket.data.userId = userAccount.id

        await prismaClient.user.update({
            where: {
                id: tokenVerifyResults
            },
            data: {
                socketId: userSocket.id
            }
        })

        userSocket.broadcast.emit('newConnection', {
            userId: userAccount.id
        })

        userSocket.on("message", async (event: MessageData | undefined) => {


            const tokenVerifyResults = verifyTokenSocketio(event?.auth?.token || '');

            if (tokenVerifyResults === "invalid" || tokenVerifyResults != userSocket.data.userId) {
                userSocket.emit('unauthenticated');
                return userSocket.disconnect();
            };

            if (!event?.data?.content || !event?.data?.toId) {
                return;
            }


            const recieverAccount = await prismaClient.user.findFirst({
                where: {
                    id: event.data.toId
                }
            })
            if (!recieverAccount) {
                return
            }

            await prisma.chat.upsert({
                update: {
                    lastMessageTime: new Date(),
                    Messages: {
                        create: {
                            authorId: userSocket.data.userId,
                            content: event.data.content,
                        }
                    }
                },
                create: {
                    lastMessageTime: new Date(),
                    id: String([userSocket.data.userId, event.data.toId].sort()),
                    Members: {
                        connect: [
                            {
                                id: userSocket.data.userId
                            },
                            {
                                id: event.data.toId
                            }
                        ]
                    },
                    Messages: {
                        create: {
                            authorId: userSocket.data.userId,
                            content: event.data.content
                        }
                    }
                },
                where: {
                    id: String([userSocket.data.userId, event.data.toId].sort())
                }
            })

            const date = new Date()
            userSocket.broadcast.to(recieverAccount.socketId).emit("message", {
                id: date,
                authorId: userSocket.data.userId,
                content: event.data.content,
                createdAt: `${date.getHours()}:${("0" + (date.getMinutes())).slice(-2)}`,
            })

        });

        userSocket.on('disconnect', async () => {
            userSocket.broadcast.emit('disconnection', {
                userId: userAccount.id
            })
        });

    };

    async read(request: Request, response: Response, socket: Server) {

        try {

            //@ts-ignore
            const userId = request.userId as string

            const chats = await prismaClient.chat.findMany({
                where: {
                    id: {
                        contains: userId
                    }
                },
                include: {
                    Messages: {
                        select: {
                            id: true,
                            authorId: true,
                            content: true,
                            createdAt: true
                        }
                    },
                    Members: {
                        where: {
                            id: {
                                not: userId
                            }
                        },
                        select: {
                            username: true,
                            imageName: true,
                            id: true,
                            socketId: true
                        }
                    }
                },
                orderBy: {
                    lastMessageTime: 'desc'
                }
            })

            const chatsData = [] as Chat[]

            await chats.forEach(async (chat) => {
                const chatOnline = await socket.sockets.sockets.get(chat.Members[0].socketId)

                //@ts-ignore
                chat.Members[0].online = 

                chat.Messages.forEach((message) => {
                    const date = new Date(Date.parse(String(message.createdAt)))
                    const formatedMessageDate = `${date.getHours()}:${("0" + (date.getMinutes())).slice(-2)}`
                    //@ts-ignore
                    message.createdAt = formatedMessageDate
                })

                chatsData.push({
                    //@ts-ignore
                    messages: chat.Messages,
                    id: chat.id,
                    //@ts-ignore
                    lastMessageTime: chat.lastMessageTime,
                    user: {
                        id: chat.Members[0].id,
                        imageUrl: `http://localhost:3000/images/${chat.Members[0].imageName}`,
                        online: chatOnline != undefined,
                        username: chat.Members[0].username
                    }
                })
            })

            return response.send({
                status: 'ok',
                chats: chatsData
            })

        } catch (error) {
            return response.json({
                status: 'error',
                message: error
            })
        }


    };
}