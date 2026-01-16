import { Injectable } from '@nestjs/common';

import { Database } from '../database';
import { ChatQueryList, Queries } from './chats.queries';

import { IMessageToServer } from '../models/chats.models';
import { ISqlSuccessResponse } from '../models/common.models';
import { newBadRequestException } from '../helpers';

const db = Database.getInstance();

@Injectable()
export class ChatsGatewayService {
  public addMessage(messageData: IMessageToServer): Promise<ISqlSuccessResponse> {
    return new Promise((resolve, reject) => {
      db.query(
        Queries.AddMessage,
        [
          messageData.chatId,
          messageData.user.userId,
          messageData.user.entryId,
          messageData.messageText,
        ],
        (error: Error, insertedMessageInfo: ISqlSuccessResponse) => {
          if (error) {
            return reject(newBadRequestException(ChatQueryList.AddMessage));
          }

          resolve(insertedMessageInfo);
        },
      );
    });
  }
}