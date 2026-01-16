"use server";
import { generateObject, JSONParseError, TypeValidationError } from "ai";
import { openai } from '@ai-sdk/openai';
import { config } from "dotenv";
import { QuestionSchema } from "~/utils/validation/schema.validation";

config({ path: ".env.local" });

export async function createPathTask(topic: string, selectedType: string[]) {
  try {
    const { object: task } = await generateObject({
      model: openai('gpt-4o'),
      prompt: `
        Generate diverse and relevant questions based on the topic: "${topic}". 

        1. If specific question types are provided, generate questions that match the following types: ${JSON.stringify(selectedType)}. 
        2. If no types are selected, mix various question types to create a diverse set of questions.
        3. Each question must adhere to the following JSON schema:
           ${JSON.stringify(QuestionSchema)}
           (Ensure each question has a unique UUID assigned to the 'id' field in the format: 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx').
        4. Ensure that all generated UUIDs are unique for each question.
        5. The questions should be well-structured and meaningful, accurately reflecting the nature of the topic and selected types. 
           Provide correct details for fields such as "title," "label," "description," and "expectedAnswer."
        6. Return the output strictly as an array of JSON objects formatted according to the specified schema.
        7. If a question type requires specific attributes (like choices for multiple-choice questions), ensure those are included.

        Thank you for your assistance!`,
      system:
        "You are a task generator! Your role is to create relevant and diverse questions based on the provided topic and types.You are responsible for generating questions based on the input topic and selected types, ensuring they conform to the schema and include valid UUIDs in the proper format.",
      schema: QuestionSchema,
      output: "array",
    });

    console.log(task);
    return task;
  } catch (error) {
    console.error("Error while generating tasks:", error);
    throw new Error("Failed to generate tasks"); 
  }
}