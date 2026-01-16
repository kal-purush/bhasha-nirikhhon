import { PrismaClient } from "@prisma/client";
import { request } from "http";
import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/db";

export async function POST(request: NextRequest) {
  const formData = await request.formData();
  const collectionName = formData.get("collection_name");
  const question = formData.get("question");
  const conversation_history = formData.get("history");
  const hyde = formData.get("hyde");
  const reranking = formData.get("reranking");

  const body = {
    collection_name: collectionName,
    question: question,
    conversation_history: conversation_history,
    hyde: hyde,
    reranking: reranking,
  };

  console.log(body);

  try {
    const response = await fetch(
      process.env.NEXT_PUBLIC_LLM_SERVER_URL + "/llm/chat_with_llm",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      }
    );

    const data = await response.json();

    return NextResponse.json({ message: data.payload }, { status: 200 });
  } catch (err) {
    return NextResponse.json(
      {
        message: "Error fetching record",
      },
      {
        status: 500,
      }
    );
  }
}