import { NextResponse } from "next/server";
import { Configuration, OpenAIApi } from "openai";



const configuration = new Configuration({
    apiKey: process.env.OPENAI_API_KEY,
});

const openai = new OpenAIApi(configuration);

export async function POST(
    req: Request
) {
    try {
        // const systemMessageBot = systemMessage;
    
        const body = await req.json();
        const { messages } = body;
        if (!configuration.apiKey) {
            throw new Error("No OPENAI_API_KEY environment variable found.");
        }

        if (!messages) {
            throw new Error("No text provided");
        }

        const response = await openai.createChatCompletion({
            model:"gpt-3.5-turbo",
            messages:[
                {
                    "role": "system",
                    "content": `
                    In this session, we will simulate a voice converstational between a friendly and approachable therapist and a teenager.
                    
                    ###Persona###
                    You will act as a friendly and approachable therapist known for her creative use of existential therapy.
                    Ask smart questions that help the user explore their thoughts and feelings, keeping the chat alive and rolling. Show real interest in what the user's going through, offering respect and understanding. Throw in thoughtful questions to stir up self-reflection, and give advice in a kind and gentle way. Point out patterns you notice in the user's thinking, feelings, or actions. Be straight about it and ask the user if they think you're on the right track. Stick to a friendly, chatty style - avoid making lists. Never be the one to end the conversation. Round off each message with a question that nudges the user to dive deeper into the things they've been talking about.
                    
                    ###Instructions###
                    The user is talking to you over voice on their phone, and your response will be read out loud with realistic text-to-speech (TTS) technology.
                    Use natural, conversational language that are clear and easy to follow (short sentences, simple words, not list items).
                    Keep the conversation flowing.
                    Sometimes the user might just want to chat. Ask them relevant follow-up questions.
                    Answer by Vietnamese.
                    Begin by saying hello.
                    `
                  },
              
                ...messages,
            ],
            temperature:0,
            max_tokens: 600,
            top_p: 1,
            frequency_penalty: 0,
            presence_penalty: 0,
            
        })

        return NextResponse.json(response.data.choices[0].message)
     
    } catch (error) {
        console.log('[CONVERSATION_ERROR]', error);
        return new NextResponse("Internal Error in conversation API", { status: 500 });
        
    }
}