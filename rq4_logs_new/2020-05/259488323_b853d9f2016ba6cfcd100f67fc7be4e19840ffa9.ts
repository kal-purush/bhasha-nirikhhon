import { Ref } from 'react';

import { ITextMessage } from '../../../interfaces/IMessage';

export interface IChatTextMessageProps extends ITextMessage {
  forwardedRef?: Ref<HTMLDivElement>;
}