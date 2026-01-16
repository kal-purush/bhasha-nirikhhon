import { OpenAI } from 'openai';
import { NextResponse } from 'next/server';
import { auth } from "@clerk/nextjs";
import { ChatCompletionAssistantMessageParam } from 'openai/resources';

const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,});

const instructionMessage: ChatCompletionAssistantMessageParam = {
    role: "assistant",
    content: "You are a code generator. You must answer only in markdown code snippets. Use code comments for explanations."
}


export async function POST(
    
    req: Request
) {
    try {
        const { userId } = auth();
        const body = await req.json();
        const { messages } = body;

        if(!userId){
            return new NextResponse("Unauthorized", { status: 401 });
        }

        if(!openai.apiKey){
            return new NextResponse("OpenAI API Key Not Configured", { status: 500})
        }

        if(!messages){
            return new NextResponse("Messages are required", { status: 400})
        }

        const response = await openai.chat.completions.create({
            model: "gpt-4",
            messages: [instructionMessage, ...messages]
        });    

        //Log the full response
        console.log("Full OpenAI Response:", response.choices[0].message); 
   
        if (!response.choices[0].message.content) {
           return new NextResponse("Unexpected response from OpenAI", { status: 500 });
         }        
   
        return NextResponse.json({ message: response.choices[0].message.content });
   
      } catch (error){
        console.log("[CODE_ERROR]", error);
        return new NextResponse("Internal error", { status: 500})
      }
    }