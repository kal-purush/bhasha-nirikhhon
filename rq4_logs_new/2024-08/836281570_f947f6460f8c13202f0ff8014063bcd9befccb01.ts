import { openai } from "@ai-sdk/openai";
import { createStreamableValue } from "ai/rsc";
import { CoreMessage, streamText, generateText } from "ai";
import dotenv from "dotenv";
import { addMeetingTool } from "@/utils/tools/addMeetingTool";

dotenv.config();
const apiKey = process.env.OPENAI_API_KEY;
const language = process.env.LANGUAGE;

export async function continueConversation(messages: CoreMessage[]) {
  "use server";

  try {
    const result = await streamText({
      model: openai("gpt-4o"),
      messages,
      system: `You're a productivity assistant and manage a daily meeting schedule.
      You should keep in mind that you manage dates, times and duration of meetings. All answers must be in ${language}.
      - You cannot schedule meetings on dates and times before the system time. 
      - If the date is not specified, take by default system time. 
      - You can schedule meetings, delete meetings, move meetings, modify meeting attendees, 
      modify the duration of meetings, modify the topics to be discussed during meetings.
      - Before scheduling a meeting, check if the time slot is available.
      - If the time slot is not available, respond with a message indicating the conflict but do not suggest an alternative time.
      - Only use \`addMeeting\` to save meetings and not for other tasks.`,
      tools: {
        addMeeting: addMeetingTool,
      },
    });

    const stream = createStreamableValue(result.text);

    return stream.value;
  } catch (error) {
    console.error("Error en POST", error);
  }
}

/*
import { openai } from "@ai-sdk/openai";
import { streamObject, generateText, tool } from "ai";
import { z } from "zod";
import dotenv from "dotenv";
import { DataMeeting } from "@/utils/interfaces";
import { addMeetingTool } from "@/utils/tools/addMeetingTool";

dotenv.config();
const apiKey = process.env.OPENAI_API_KEY;
const language = process.env.LANGUAGE;

export async function POST(request: Request) {
  const { task }: { task: string } = await request.json();
  const today = new Date();
  console.log("TODAY", today);

  try {
    const { roundtrips, text } = await generateText({
      model: openai("gpt-4o"),

      maxToolRoundtrips: 10,

      system: `You're a productivity assistant and manage a daily meeting schedule.
      You should keep in mind that you manage dates, times and duration of meetings. All answers must be in ${language}.
      - You cannot schedule meetings on dates and times before ${today}. 
      - If the date is not specified, take by default ${today}. 
      - You can schedule meetings, delete meetings, move meetings, modify meeting attendees, 
      modify the duration of meetings, modify the topics to be discussed during meetings.
      - Before scheduling a meeting, check if the time slot is available.
      - If the time slot is not available, respond with a message indicating the conflict but do not suggest an alternative time.
      - Only use \`addMeeting\` to save meetings and not for other tasks.`,

      prompt: `The task to do now is ${task}`,

      tools: {
        addMeeting: addMeetingTool,
      },
    });

    const allToolCalls = roundtrips.flatMap((roundtrip) => roundtrip.toolCalls);

    console.log("Mirando args flatMap", allToolCalls);
    console.log("TEXT", text);
    console.log("Roundtrip", roundtrips);
    console.log(
      "Roundtrip de length - toolResult",
      roundtrips[roundtrips.length - 1].toolResults
    );
    console.log(
      "Roundtrip de cero - result",
      roundtrips[0].toolResults[0].result.success
    );
    const array: any[] = [];

    //if (!allToolCalls[0]) {
    if (!roundtrips[0].toolResults[0].result.success) {
      array.push({
        message: text,
        what: [""],
        who: [""],
        when: "",
        since: "",
        until: "",
        about: [""],
        duration: "",
      });
    } else {
      const temp = allToolCalls[0].args.dataMeeting;
      array.push(temp);
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_BASE_URL}/api/tasks`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(array[0]),
        }
      );
      const data = await response.json();
    }

    console.log("Resultado final", array[0]);
    const retorno = new Response(JSON.stringify(array), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
      },
    });

    return retorno;
  } catch (error) {
    console.error("Error en POST", error);
    return new Response("Internal Server Error", { status: 500 });
  }
}

*/