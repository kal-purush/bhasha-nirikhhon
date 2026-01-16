"use server";

import * as z from "zod";
import { zodResolver } from "@hookform/resolvers/zod";
import { ChatGoogleGenerativeAI } from "@langchain/google-genai";
import { revalidatePath } from "next/cache";

const model = new ChatGoogleGenerativeAI({
  modelName: "gemini-pro",
  apiKey: process.env.GOOGLE_AI_API_KEY,
  temperature: 0.8,
});

export async function generateForm(
  prevState: { message: string },
  formData: FormData
) {
  const promptTemplate =
    "Based on the description, generate a survey with questions array where every element has 2 fields: text and the fieldType and fieldType can be of these options RadioGroup, Select,Input, Textarea, Switch and return it in json format. For RadioGroup, and Select types also return fieldOptions array with text and value fields. For example, for RadioGroup, and Select types, the field options array can be [{text:'Yes', value:'yes'}, {text:'no', value:'no'}] and for input, textarea and switch types, the field options array can be empty. For example, for input, textarea and switch types, the field options array can be []";
  const schema = z.object({
    description: z.string().min(1),
  });

  const parse = schema.safeParse({
    description: formData.get("description"),
  });

  if (!parse.success) {
    console.log(parse.error);
    return {
      message: "Failed to parse data",
    };
  }

  const descriptionData = parse.data;

  if (!process.env.GOOGLE_AI_API_KEY) {
    return {
      message: "API key not found",
    };
  }

  try {
    const data = await model.invoke([
      [
        "system",
        `${descriptionData.description}  ${
          descriptionData.description as string
        }`,
      ],
    ]);
    console.log(data);

    revalidatePath("/");

    return {
      message: "Success",
      data: data,
    };
  } catch (error) {
    return { message: "Failed to create form" };
  }
}