import { axiosInstance } from '@/lib/axios';
import type { SendChatArgument } from '../hooks/useSendChat';

export const postChatSSE = ({
  message,
  threadId,
  onMessage,
}: SendChatArgument & {
  onMessage: (text: string, threadId?: string) => void;
}) => {
  const optionalQuery = threadId ? `&thread_id=${threadId}` : '';

  return axiosInstance.get(`/chat/sse?text=${message}${optionalQuery}`, {
    headers: {
      'Content-Type': 'application/json',
      Accept: 'text/event-stream',
    },
    responseType: 'stream',
    onDownloadProgress: (progressEvent) => {
      const chunk = progressEvent.event.target.response;
      if (!chunk) return;

      const lines = chunk
        .split('data: ')
        .map((line: string) => line.trim())
        .filter((s: string) => s);

      for (const json of lines) {
        try {
          if (json === '[DONE]') return;
          const data = JSON.parse(json);
          onMessage(data.content, data.thread_id);
        } catch (error) {
          console.error('Error parsing JSON:', error);
        }
      }
    },
  });
};