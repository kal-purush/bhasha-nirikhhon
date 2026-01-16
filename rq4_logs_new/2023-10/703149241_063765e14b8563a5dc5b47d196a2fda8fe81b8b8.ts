import axios from "axios";
import { SqlToolkit, createSqlAgent } from "langchain/agents/toolkits/sql";
import { ConversationChain, LLMChain } from "langchain/chains";
import { SqlDatabaseChain } from "langchain/chains/sql_db";
import { ChatOpenAI, ChatOpenAICallOptions } from "langchain/chat_models/openai";
import { OpenAI } from "langchain/llms/openai";
import { BufferMemory } from "langchain/memory";
import { PromptTemplate } from "langchain/prompts";
import { BaseMessageChunk, ChainValues } from "langchain/schema";
import { StringOutputParser } from "langchain/schema/output_parser";
import { RunnableSequence } from "langchain/schema/runnable";
import { SqlDatabase } from "langchain/sql_db";
import { NextRequest, NextResponse } from "next/server";
import { DataSource } from "typeorm";

export async function GET(request: NextRequest) {
  try {
    const { data } = await axios("https://jsonplaceholder.typicode.com/users");
    console.log("[USERS_GET]", data);
    return NextResponse.json(data);
  } catch (error) {
    console.log("[USERS_GET]", error);
    return new NextResponse("Internal Error", { status: 500 });
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    console.log("[USERS_POST]", body);

    const datasource = new DataSource({
      type: "sqlite",
      database: "./public/Contructions.db",
    });


    const db = await SqlDatabase.fromDataSourceParams({
      appDataSource: datasource,
    });

    const model = new OpenAI({ 
      openAIApiKey: "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",
      modelName: "gpt-3.5-turbo",
      temperature: 0,
     });

     const template = `Given an input question, first create a syntactically correct {dialect} query to run, then look at the results of the query and return the answer.
      Use the following format:

      Question: "Question here"
      SQLQuery: "SQL Query to run"
      SQLResult: "Result of the SQLQuery"
      Answer: "Final answer here"

      If is not a valid question provide a natural language response.

      If is not a question try to be kind and provide a natural language response.

      Question: {input}`;

    const prompt = PromptTemplate.fromTemplate(template);

     const chain = new SqlDatabaseChain({
      llm: model,
      database: db,
      sqlOutputKey: "sql",
      prompt
    });
    try {
      
      const res = await chain.call({ query: body.message });
      console.log('res', res);
      return NextResponse.json({
          message: body.message,
          ai: res.result,
      });
    } catch (error) {
      console.log('error', error);
      
    }
    

  } catch (error) {
    console.log("[USERS_POST]", error);
    return new NextResponse("Internal Error", { status: 500 });
  }
}
/* export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    console.log("[USERS_POST]", body);

    const datasource = new DataSource({
      type: "sqlite",
      database: "./public/Contructions.db",
    });


    const db = await SqlDatabase.fromDataSourceParams({
      appDataSource: datasource,
    });

    const model = new OpenAI({ 
      openAIApiKey: "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",
      modelName: "gpt-3.5-turbo",
      temperature: 0,
     });
  const toolkit = new SqlToolkit(db, model);
  const executor = createSqlAgent(model, toolkit);

   /*  const schema = await db.getTableInfo();
    console.log('schema', schema);
    
    if (!schema) {
      throw new Error("No schema found");
      return;
    } 

    const result = await executor.call({ input: body.message });
    console.log('result', result);
    

    return NextResponse.json({
        message: body.message,
        ai: result.output,
    });
  } catch (error) {
    console.log("[USERS_POST]", error);
    return new NextResponse("Internal Error", { status: 500 });
  }
}
 */