import { Conversation } from "@/lib/conversation";
import { getMatchesFromEmbeddings, Metadata } from "@/lib/matches";
import { summarizeLongDocument } from "@/lib/summarizer";
import { templates } from "@/lib/templates";
import { PineconeClient } from "@pinecone-database/pinecone";
import { LLMChain } from "langchain/chains";
import { ChatOpenAI } from "langchain/chat_models/openai";
import { OpenAIEmbeddings } from "langchain/embeddings/openai";
import { OpenAI } from "langchain/llms/openai";
import { PromptTemplate } from "langchain/prompts";
import { NextRequest, NextResponse } from "next/server";

export async function POST(request: NextRequest) {
  const body = await request.json();
  return handleRequest({ prompt: body.prompt });
}

const llm = new OpenAI({
  openAIApiKey: process.env.OPENAI_API_KEY,
  modelName: "gpt-3.5-turbo",
  temperature: 0,
});
let pinecone: PineconeClient | null = null;

const initPineconeClient = async () => {
  pinecone = new PineconeClient();
  await pinecone.init({
    environment: process.env.PINECONE_ENVIRONMENT!,
    apiKey: process.env.PINECONE_API_KEY!,
  });
};

const handleRequest = async ({
  prompt,
}: {
  prompt: string;
}) => {
  try {
    if (!pinecone) {
      await initPineconeClient();
    }

    let summarizedCount = 0;
    // Retrieve the conversation log and save the user's prompt
    const conversation = new Conversation();
    const conversationHistory = await conversation.getConversation({
      limit: 10,
    });
    await conversation.addEntry({ entry: prompt, speaker: "user" });

    // Build an LLM chain that will improve the user prompt
    const inquiryChain = new LLMChain({
      llm,
      prompt: new PromptTemplate({
        template: templates.inquiryTemplate,
        inputVariables: ["userPrompt", "conversationHistory"],
      }),
    });
    const inquiryChainResult = await inquiryChain.call({
      userPrompt: prompt,
      conversationHistory,
    });
    const inquiry = inquiryChainResult.text;

    console.log(inquiry);

    console.log(inquiry);

    // Embed the user's intent and query the Pinecone index
    const embedder = new OpenAIEmbeddings({
      modelName: "text-embedding-ada-002",
      openAIApiKey: process.env.OPENAI_API_KEY,
    });

    const embeddings = await embedder.embedQuery(inquiry);
    const matches = await getMatchesFromEmbeddings(embeddings, pinecone!, 2);

    const urls =
      matches &&
      Array.from(
        new Set(
          matches.map((match) => {
            const metadata = match.metadata as Metadata;
            const { url } = metadata;
            return url;
          })
        )
      );

    console.log(urls);

    const docs =
      matches &&
      Array.from(
        matches.reduce((map, match) => {
          const metadata = match.metadata as Metadata;
          const { text, url } = metadata;
          if (!map.has(url)) {
            map.set(url, text);
          }
          return map;
        }, new Map())
      ).map(([_, text]) => text);

    const promptTemplate = new PromptTemplate({
      template: templates.qaTemplate,
      inputVariables: ["summaries", "question", "conversationHistory", "urls"],
    });

    const chat = new ChatOpenAI({
      modelName: "gpt-3.5-turbo",
      openAIApiKey: process.env.OPENAI_API_KEY,
    });

    const chain = new LLMChain({
      prompt: promptTemplate,
      llm: chat,
    });

    const allDocs = docs.join("\n");
    const summary =
      allDocs.length > 4000
        ? await summarizeLongDocument({ document: allDocs, inquiry })
        : allDocs;

    const response = await chain.call({
      summaries: summary,
      question: prompt,
      conversationHistory,
      urls,
    });

    console.log("response", response);
    return NextResponse.json({
      message: prompt,
      ai: response.text,
  });
  } catch (error) {
    console.log("error", error);
  }
};