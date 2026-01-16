import fetch from 'node-fetch';
import { NextResponse } from 'next/server';

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const OPENROUTER_BASE_URL = 'https://openrouter.ai/api/v1/chat/completions';

export async function POST(request) {
    try {
        console.log('API, Received request');
        const data = await request.text();
        console.log('API, Text passed');

        // const fileContents = await Promise.all(
        //     files.map(async (file) => {
        //         console.log('API, Processing file:', file.name);
        //         return {
        //             name: file.name,
        //             content: await file.text(),
        //         };
        //     })
        // );

        // console.log('API, File contents:', fileContents);

        // ${fileContents.map(({ name, content }) => `\n**File: ${name}**\n${content}`).join("\n")}


        const prompt = `Analyze the following web code and add ARIA enhancements just to the code to improve its accessibility:
            ${data}).join("\n")}
        No need to give any explanation or anything, just the code would be fine. Thanks.
        `;

        const response = await fetch(OPENROUTER_BASE_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${OPENROUTER_API_KEY}`,
            },
            body: JSON.stringify({
                model: "deepseek/deepseek-r1-distill-llama-70b:free",
                messages: [
                    { role: "system", content: "You are an expert in web accessibility and ARIA best practices." },
                    { role: "user", content: prompt },
                ],
            }),
        });

        const responseData = await response.json();
        if (!response.ok) {
            console.error('OpenRouter API error:', responseData);
            return NextResponse.json(
                { error: "Failed to fetch AI response", details: responseData },
                { status: 500 }
            );
        }

        return NextResponse.json({
            suggestions: responseData.choices[0]?.message?.content || "No suggestions found."
        });

    } catch (error) {
        console.error('Detailed error:', error);
        console.error('Server error:', error);
        return NextResponse.json(
            { error: "Internal Server Error", details: error.message },
            { status: 500 }
        );
    }
}

export const config = {
    api: {
        bodyParser: false,
    },
}; 