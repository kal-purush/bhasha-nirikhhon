import { ChatMessage } from '../models';

export interface LogChatMessage{
    messages:ChatMessage[];
    newMessages:number;
}