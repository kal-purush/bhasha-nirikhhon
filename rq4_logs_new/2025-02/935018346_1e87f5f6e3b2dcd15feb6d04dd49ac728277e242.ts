import { openai } from '@ai-sdk/openai';
import { streamText, type Message } from 'ai';
import { api } from '~/trpc/server';

// Allow streaming responses up to 30 seconds
export const maxDuration = 30;

export async function POST(req: Request) {
  const { messages }: { messages: Message[] } = await req.json();

  const result = streamText({
    model: openai('gpt-4o'),
    messages,
    onError: async (error) => {
      console.error(error);
    },
    onFinish: async (result) => {
      const sessionId = await api.sessions.createSession();
      console.log(sessionId);
      if (!sessionId) {
        throw new Error("Session ID is undefined");
      }
      const messageId = await api.messages.storeMessage({
        sessionId,
        role: "assistant",
        content: result.text,
      });
      console.log(messageId);
    },
  });

  return result.toDataStreamResponse();
}