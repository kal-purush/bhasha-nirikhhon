import { NextRequest, NextResponse } from "next/server";
import { model } from "@/utils/chatService";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { userResponse } = body;

    const dynamicPrompt = `
    You are Octo, the friendly octopus. 
    You are to play the role of a chatbot to help the user determine what tasks to add to their to do list.

    Based on the user's most recent response ${userResponse} generate a response to continue the conversation. 
    Answer any questions the user may have. 
    When listing steps, use numbered bullet points and put each step in a newline.
    Be polite but fun. 
    Keep response short and concise.
    `;

    const response = await model.generateContent(dynamicPrompt);
    console.log("Fetched reponse from Gemini: ", response);

    const octosResponseText =
      response.response?.candidates[0]?.content.parts[0].text ||
      "Error generating response.";

    return NextResponse.json({
      octosResponse: octosResponseText,
    });
  } catch (error) {
    console.error("Error generating response:", error);
    // res.status(500).json({ error: "Internal Server Error" });
    return NextResponse.json(
      { error: "Internal Server Error" },
      { status: 500 }
    );
  }
}