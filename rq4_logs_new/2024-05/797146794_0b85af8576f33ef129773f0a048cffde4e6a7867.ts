import { OpenAIClient, OpenAIKeyCredential } from "@azure/openai";
import { NextRequest, NextResponse } from "next/server";

export async function POST(request: NextRequest) {
  const formData = await request.formData();
  const file = formData.get("audio") as File;

  console.log(file);

  if (
    process.env.AZURE_APT_KEY === undefined ||
    process.env.AZURE_ENDPOINT === undefined ||
    process.env.AZURE_DEPLOYMENT_NAME
  ) {
    console.error("Azure credentials not set");
    return {
      sender: "",
      response: "Azure credentials not set",
    };
  }

  if (file.size === 0) {
    return {
      sender: "",
      response: "No Audio file provided",
    };
  }

  const arrayBuffer = await file.arrayBuffer();
  const audio = new Uint8Array(arrayBuffer);

  const client = new OpenAIClient(
    process.env.AZURE_ENDPOINT,
    new OpenAIKeyCredential(process.env.AZURE_APT_KEY)
  );

  const result = await client.getAudioTranscription(
    process.env.AZURE_DEPLOYMENT_NAME as string,
    audio
  );

  console.log(`Transcription: ${result.text}`)

  return NextResponse.json({text: result.text})
}