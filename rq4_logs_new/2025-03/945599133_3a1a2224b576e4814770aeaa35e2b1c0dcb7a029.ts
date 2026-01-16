import { NextResponse } from 'next/server';
import OpenAI from 'openai';

const openai = new OpenAI({
  apiKey: process.env.NEXT_PUBLIC_OPENAI_API_KEY || '',
});

const SYSTEM_PROMPT = `You are a knowledgeable AI assistant for the Sonic blockchain. You have expertise in:
- Sonic's technical features (10,000 TPS, sub-second finality, EVM compatibility)
- Token economics (S token, FTM conversion, staking)
- Ecosystem components (Gateway, bridges, DeFi applications)
- Developer incentives (Fee Monetization, Innovator Fund)
- User incentives (Airdrops, staking rewards)

Combine this knowledge with your general blockchain and cryptocurrency expertise to provide helpful, accurate responses.
Keep responses concise but informative. If you're not sure about something, be honest about it.`;

export async function POST(request: Request) {
  try {
    const { messages } = await request.json();

    const response = await openai.chat.completions.create({
      model: "gpt-3.5-turbo",
      messages: [
        { role: "system", content: SYSTEM_PROMPT },
        ...messages
      ],
      temperature: 0.7,
      max_tokens: 500,
    });

    return NextResponse.json({
      content: response.choices[0].message.content
    });
  } catch (error) {
    console.error('Chatbot API error:', error);
    return NextResponse.json(
      { error: 'Failed to get response from AI' },
      { status: 500 }
    );
  }
} 