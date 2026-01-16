
import { Conversation } from "@/lib/conversation";
import { Metadata, getMatchesFromEmbeddings } from "@/lib/matches";
import { summarizeLongDocument } from "@/lib/summarizer";
import { templates } from "@/lib/templates";
import { PineconeClient } from "@pinecone-database/pinecone";
import { ConversationChain, LLMChain } from "langchain/chains";
import { ChatOpenAI, ChatOpenAICallOptions } from "langchain/chat_models/openai";
import { OpenAIEmbeddings } from "langchain/embeddings/openai";
import { OpenAI } from "langchain/llms/openai";
import { ChatPromptTemplate, MessagesPlaceholder, PromptTemplate } from "langchain/prompts";
import { AIMessage, BaseMessageChunk, ChainValues, HumanMessage } from "langchain/schema";
import { StringOutputParser } from "langchain/schema/output_parser";
import { RunnableSequence } from "langchain/schema/runnable";
import { SqlDatabase } from "langchain/sql_db";
import { NextRequest, NextResponse } from "next/server";
import { DataSource } from "typeorm";
import { chatOpenAI, embedder, initPineconeClient, llm } from '../../../lib/initializations';
import { BufferMemory, ChatMessageHistory } from "langchain/memory";

export const maxDuration = 300;
export async function POST(request: NextRequest) {
  console.time("POST");
  const body = await request.json();

  const model: ChatOpenAI<ChatOpenAICallOptions> = new ChatOpenAI({
    openAIApiKey: process.env.OPENAI_API_KEY,
    modelName: "gpt-3.5-turbo",
    temperature: 0,
  });

  // return await chatWithHistory(model, body.message);

  const questionGeneratorTemplate = PromptTemplate.fromTemplate(
    `Given the following conversation and a follow up question, rephrase the follow up question base on the chat history adding the missing data.
  ----------
  CHAT HISTORY: {chatHistory}
  ----------
  FOLLOWUP QUESTION: {question}
  ----------
  Standalone question:`
  );
  const fasterChain = new LLMChain({
    llm: model,
    prompt: questionGeneratorTemplate,
  });

  const { text } = await fasterChain.invoke({
    chatHistory: JSON.stringify(body.history),
    context: JSON.stringify(body.history),
    question: body.message,
  });

  console.log('text-generated', text);
  


  try {
    const datasource = new DataSource({
      type: "postgres",
      host: process.env.POSTGRES_DB_HOST,
      port: Number(process.env.POSTGRES_DB_PORT),
      username: process.env.POSTGRES_DB_USER,
      password: process.env.POSTGRES_DB_PASSWORD,
      database: process.env.POSTGRES_DB_DATABASE,
    });
// console.log('datasource', datasource);

    const db = await SqlDatabase.fromDataSourceParams({
      appDataSource: datasource,
      ignoreTables: ["conversations"],
    });

    const schema = await db.getTableInfo();
    /* console.log('schema', schema);
    await fs.writeFile('./schema.json', schema);
    

    if (!schema) {
      throw new Error("No schema found");
      return;
    } */
    

    const prompt =
      PromptTemplate.fromTemplate(`Based on the table schema and chat history below, write a postgres SQL query that would answer the user's question and do not mention that you do a sql query in the answer, if you do not have all data necessary to write:
{schema}
{history}

Question: {question}
SQL Query:`);

console.log('history', JSON.stringify(body.history));


    const sqlQueryGeneratorChain = RunnableSequence.from([
      {
        schema: async () => schema,
        history: () => JSON.stringify(body.history),
        question: (input) => input.question,
      },
      prompt,
      model.bind({ stop: ["\nSQLResult:"] }),
      new StringOutputParser(),
    ]);

    const result = await sqlQueryGeneratorChain.invoke({
      question: body.message,
    });

    console.log('query', result);

    if (!result.startsWith('SELECT')) {
        return await handleRequest({prompt: body.message});
    }

    const finalResponsePrompt =
      PromptTemplate.fromTemplate(`Based on the table schema below, question, postgres sql query, and sql response, write a natural language response, is the response is a list or table, please write a response in markdown format when show the list or table in a separate line, if there are no data, please write a short message to express that there are no data:
        {schema}

        Question: {question}
        SQL Query: {query}
        SQL Response: {response}`);

    const fullChain = RunnableSequence.from([
      {
        question: (input) => input.question,
        query: sqlQueryGeneratorChain,
      },
      {
        schema: async () => templates.dbSchema,
        question: (input) => input.question,
        query: (input) => input.query,
        response: (input) => {
          console.time("SQL");
          return db.run(input.query)
            .then((res) => {
              console.log('res', res);
              
              console.timeEnd("SQL");
              console.time("OpenAI");
              return res;
            })
            .catch((err) => {
              console.timeEnd("SQL");
              return err;
            });
        },
      },
      finalResponsePrompt,
      model,
    ]);

    const finalResponse: BaseMessageChunk = await fullChain.invoke({
      question: body.message,
    });
    console.log('final response', finalResponse.content);
console.timeEnd("POST");
    return NextResponse.json({
        message: body.message,
        ai: finalResponse.content
    });
  } catch (error) {
    console.log("[USERS_POST123]", error);
    return NextResponse.json({
      message: body.message,
      ai: 'Sorry, I am not able to answer your question.'
  });
  }
}

const chatWithHistory = async (model: ChatOpenAI<ChatOpenAICallOptions>, question: string) => {
  const pastMessages = [
    new HumanMessage("What is the name of the first project?"),
    new AIMessage("The first project name is: Project 10."),
   /*  new HumanMessage("Give me the tasks of the project."),
    new AIMessage("To retrieve the tasks of a specific project, I need the project ID. Could you please provide me with the project ID?"), */
  ];
  const memory = new BufferMemory({
    chatHistory: new ChatMessageHistory(pastMessages),
  });

  const prompt = PromptTemplate.fromTemplate(templates.conversationTemplate);

  const chatPrompt = ChatPromptTemplate.fromMessages([
    [
      "system",
      `The following is a conversation between a human and an AI. The AI is an expert in sql and will create an sql query to answer the human's question following the rules bellow:
        - The AI will only answer if the question is a valid sql query.
        - The AI will complete the query base on the conversation history.
        - The AI will use the following database schema: 
        Database schemaCREATE TABLE "public"."conversations" (
        id bigint NOT NULL, created_at timestamp with time zone NOT NULL, entry character varying , speaker character varying ) 
        SELECT * FROM "public"."conversations" LIMIT 3;
         id created_at entry speaker
        CREATE TABLE "public"."logic" (
        logic_id integer NOT NULL, prerequisite_task_id integer , dependent_task_id integer ) 
        SELECT * FROM "public"."logic" LIMIT 3;
         logic_id prerequisite_task_id dependent_task_id
         1 1 2
         2 2 3
         3 3 4
        CREATE TABLE "public"."projects" (
        project_id integer NOT NULL, project_name character varying ) 
        SELECT * FROM "public"."projects" LIMIT 3;
         project_id project_name
         1 High-rise Building Construction Project
         2 Residential Complex Construction Project
         3 Bridge Construction Project
        CREATE TABLE "public"."resource_assignments" (
        assignment_id integer NOT NULL, task_id integer , resource_name character varying , assignment_duration integer ) 
        SELECT * FROM "public"."resource_assignments" LIMIT 3;
         assignment_id task_id resource_name assignment_duration
         1 1 Excavator Operator 2
         2 2 Construction Worker 2
         3 3 Bricklayer 2
        CREATE TABLE "public"."tasks" (
        task_id integer NOT NULL, task_name character varying , project_id integer , commencement_date date , conclusion_date date , duration integer ) 
        SELECT * FROM "public"."tasks" LIMIT 3;
         task_id task_name project_id commencement_date conclusion_date duration
         1 Excavation 1 Wed Oct 11 2023 00:00:00 GMT-0500 (Eastern Standard Time) Sat Oct 14 2023 00:00:00 GMT-0500 (Eastern Standard Time) 3
         2 Foundation Construction 1 Wed Oct 18 2023 00:00:00 GMT-0500 (Eastern Standard Time) Sat Oct 21 2023 00:00:00 GMT-0500 (Eastern Standard Time) 3
         3 Wall Construction 1 Wed Oct 25 2023 00:00:00 GMT-0500 (Eastern Standard Time) Sat Oct 28 2023 00:00:00 GMT-0500 (Eastern Standard Time) 3
      
          - The AI response with the sql query that answers the question
          - The AI will not mention that it is doing a sql query
          - The AI will not mention the database schema
          - The AI return only the sql query
         `,
    ],
    new MessagesPlaceholder("history"),
    ["human", "{input}"],
  ]);
  
  const chain = new ConversationChain({ llm: model, memory, prompt: chatPrompt });
  const response = await chain.call({ input: question});
  console.log('response', response);
  return NextResponse.json({
    message: question,
    ai: response.response
});
};

async function chat(input: string, model: ChatOpenAI<ChatOpenAICallOptions>) {
    const template = "Respond the question with short answer and be nice\n Question: {question}";

    const prompt = new PromptTemplate({ template, inputVariables: ["question"] });

    const chain = new LLMChain({ llm: model, prompt });

    const result: ChainValues = await chain.call({ question: input });
    console.log('result', result);
    
    return  {
        message: input,
        ai: result.text.replaceAll('Answer: ', '')
    }
}

const handleRequest = async ({
  prompt
}: {
  prompt: string;
}) => {
  try {
    const pinecone = await initPineconeClient()

    // let summarizedCount = 0;
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

    // Embed the user's intent and query the Pinecone index
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

    const chain = new LLMChain({
      prompt: promptTemplate,
      llm: chatOpenAI,
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