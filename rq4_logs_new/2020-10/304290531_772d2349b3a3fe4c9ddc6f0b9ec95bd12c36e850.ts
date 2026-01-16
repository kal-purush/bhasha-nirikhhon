/**
 * 处理 background 发来的信息
 */
import logger from '@/utils/logger';
import { MessagePayloadMap, MessageResponseMap, MessageType } from '@/types/message';
import { modifyRemainingTime, showVideo } from './showVideo';

export default function handleMessage(
  message: {
    type: MessageType;
    payload: MessageResponseMap[MessageType];
  },
  sender: chrome.runtime.MessageSender,
  sendResponse: <K extends MessageType>(response: MessagePayloadMap[K]) => void,
) {
  switch (message.type) {
    case 'fetchVideo':
      {
        logger.info('chontent: received fetchVideo');
        const payload = message.payload as MessageResponseMap['fetchVideo'];
        if (payload !== null) {
          showVideo(payload);
        }
      }
      break;
    case 'synchronizeTime':
      {
        logger.info('chontent: received synchronizeTime');
        const payload = message.payload as MessageResponseMap['synchronizeTime'];
        if (payload !== null) {
          modifyRemainingTime(payload);
        }
      }
      break;
    default:
      break;
  }
  return true;
}