import { NextResponse } from 'next/server';
import { getChatbotResponse } from '@/services/chatbot-service';

export async function POST(request: Request) {
    try {
        const { userMessage } = await request.json();

        if (!userMessage) {
            return NextResponse.json({ message: 'User message is required' }, { status: 400 });
        }

        const responseMessage = await getChatbotResponse(userMessage);
        return NextResponse.json({ message: responseMessage });

    } catch (error) {
        return NextResponse.json({ message: 'Error processing request', error: (error as any).message }, { status: 500 });
    }
}