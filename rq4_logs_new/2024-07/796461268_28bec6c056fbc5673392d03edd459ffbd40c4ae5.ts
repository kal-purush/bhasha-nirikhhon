import { PrismaClient } from "@prisma/client";
import { request } from "http";
import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/db";
import { auth } from "@clerk/nextjs/server";

export async function POST(request: NextRequest) {
  const formData = await request.formData();
  const question = formData.get("question");
  const conversation_history = formData.get("history");
  const hyde = formData.get("hyde");
  const reranking = formData.get("reranking");
  const { userId } = auth();

  const body = {
    user_id: userId,
    question: question,
    conversation_history: conversation_history,
    hyde: hyde,
    reranking: reranking,
  };

  try {
    const response = await fetch(
      process.env.LLM_SERVER_URL + "/llm/chat_with_llm",
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