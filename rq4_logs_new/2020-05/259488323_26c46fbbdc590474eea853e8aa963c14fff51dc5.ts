import { ITextMessageDto, ITextMessage } from '../models/interfaces/IMessage';

import { removeTimeFromTimestamp } from './dateHelper';

export const mapTextMessageDtoToTextMessage = (arr: ITextMessageDto[]): ITextMessage[] => {
  return arr.map((dto, i) => {
    const message: ITextMessage = { ...dto, isChained: false };
    const prevMessage = arr[i - 1];
    const isSameAuthor = prevMessage?.author.guid === dto.author.guid;
    const isSameDate = removeTimeFromTimestamp(prevMessage?.timestamp) === removeTimeFromTimestamp(dto.timestamp);
    if (isSameAuthor && isSameDate) {
      message.isChained = true;
    }
    return message;
  });
};