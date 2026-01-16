import Bottleneck from "bottleneck";
import { OpenAIEmbeddings } from "langchain/embeddings/openai";
import { Document } from "langchain/document";
import { TokenTextSplitter } from "langchain/text_splitter";

import { PineconeClient, Vector } from "@pinecone-database/pinecone";
import { NextRequest, NextResponse } from "next/server";
import { Crawler, Page } from "@/lib/crawler/crawler";
import { v4 as uuidv4 } from 'uuid';
import { summarizeLongDocument } from "@/lib/summarizer";

const limiter = new Bottleneck({
  minTime: 50,
});

let pinecone: PineconeClient | null = null;

const initPineconeClient = async () => {
  pinecone = new PineconeClient();
  console.log("init pinecone");
  await pinecone.init({
    environment: process.env.PINECONE_ENVIRONMENT!,
    apiKey: process.env.PINECONE_API_KEY!,
  });
};

type Response = {
  message: string;
};

// The TextEncoder instance enc is created and its encode() method is called on the input string.
// The resulting Uint8Array is then sliced, and the TextDecoder instance decodes the sliced array in a single line of code.
const truncateStringByBytes = (str: string, bytes: number) => {
  const enc = new TextEncoder();
  return new TextDecoder("utf-8").decode(enc.encode(str).slice(0, bytes));
};

const sliceIntoChunks = (arr: Vector[], chunkSize: number) => {
  return Array.from({ length: Math.ceil(arr.length / chunkSize) }, (_, i) =>
    arr.slice(i * chunkSize, (i + 1) * chunkSize)
  );
};

export async function GET(req: NextRequest, res: NextResponse) {
  if (!process.env.PINECONE_INDEX_NAME) {
    return NextResponse.json(
      { message: "PINECONE_INDEX_NAME not set" },
      { status: 500 }
    );
  }

  const params = req.nextUrl.searchParams;
  console.log('params', params);
  const urls = params.get('urls')!.split(",");

  const crawlLimit = parseInt(params.get('limit') as string) || 100;
  const pineconeIndexName =
    (params.get('indexName') as string) || process.env.PINECONE_INDEX_NAME!;
  const shouldSummarize = params.get('summmarize') === "true";

  if (!pinecone) {
    await initPineconeClient();
  }
   const indexes = pinecone && (await pinecone.listIndexes());
   console.log('indexes', indexes);
   
  if (!indexes?.includes(pineconeIndexName)) {
    NextResponse.json(
      {
        message: `Index ${pineconeIndexName} does not exist`,
      },
      { status: 500 }
    );
    throw new Error(`Index ${pineconeIndexName} does not exist`);
  }
  console.log('pineconeIndexName', pineconeIndexName);
  

  const crawler = new Crawler(urls, crawlLimit, 200);
 /* const pages = (await crawler.start()) as Page[];
  console.log('pages', pages);
  

  const documents = await Promise.all(
    pages.map(async (row) => {
      const splitter = new TokenTextSplitter({
        encodingName: "gpt2",
        chunkSize: 300,
        chunkOverlap: 20,
      });

      const pageContent = shouldSummarize
        ? await summarizeLongDocument({ document: row.text })
        : row.text;

      const docs = splitter.splitDocuments([
        new Document({
          pageContent,
          metadata: {
            url: row.url,
            text: truncateStringByBytes(pageContent, 36000),
          },
        }),
      ]);
      return docs;
    })
  );

  const index = pinecone && pinecone.Index(pineconeIndexName);

  const embedder = new OpenAIEmbeddings({
    modelName: "text-embedding-ada-002",
  });
  let counter = 0;

  //Embed the documents
  const getEmbedding = async (doc: Document) => {
    const embedding = await embedder.embedQuery(doc.pageContent);
    console.log(doc.pageContent);
    console.log("got embedding", embedding.length);
    process.stdout.write(
      `${Math.floor((counter / documents.flat().length) * 100)}%\r`
    );
    counter = counter + 1;
    return {
      id: uuidv4(),
      values: embedding,
      metadata: {
        chunk: doc.pageContent,
        text: doc.metadata.text as string,
        url: doc.metadata.url as string,
      },
    } as Vector;
  };
  const rateLimitedGetEmbedding = limiter.wrap(getEmbedding);
  process.stdout.write("100%\r");
  console.log("done embedding");

  let vectors = [] as Vector[];

  try {
    vectors = (await Promise.all(
      documents.flat().map((doc) => rateLimitedGetEmbedding(doc))
    )) as unknown as Vector[];
    const chunks = sliceIntoChunks(vectors, 10);
    console.log(chunks.length);

    try {
      await Promise.all(
        chunks.map(async (chunk) => {
          await index!.upsert({
            upsertRequest: {
              vectors: chunk as Vector[],
              namespace: "",
            },
          });
        })
      );

      NextResponse.json({ message: "Done" }, { status: 200 });
    } catch (e) {
      console.log(e);
      NextResponse.json(
        { message: `Error ${JSON.stringify(e)}` },
        { status: 500 }
      );
    }
  } catch (e) {
    console.log(e);
  } */
}