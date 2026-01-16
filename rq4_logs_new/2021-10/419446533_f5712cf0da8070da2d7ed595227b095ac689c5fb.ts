// import { io } from '../app';
import prismaClient from '../prisma';

type Request = {
  text: string;
  user_id: string;
}

type NewMessageEventPayload = {
  text: string;
  created_at: Date;
  user: {
    id: string;
    name: string;
    avatar_url: string;
  };
}

export class CreateMessageService {
  async execute(data: Request) {
    const { text, user_id } = data;

    const message = await prismaClient.message.create({
      data: {
        text,
        user_id,
      },
      include: {
        user: true,
      },
    });

    const payload: NewMessageEventPayload = {
      text: message.text,
      created_at: message.created_at,
      user: {
        id: message.user.id,
        name: message.user.name,
        avatar_url: message.user.avatar_url,
      }
    }

    // TODO: Emitir o evento de nova mensagem criada
    // io.emit('new_message', payload);

    return message;
  }
}