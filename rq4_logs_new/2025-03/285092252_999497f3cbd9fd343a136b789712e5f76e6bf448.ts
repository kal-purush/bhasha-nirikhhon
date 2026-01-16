import { handle } from "hono/aws-lambda";
import { create } from "opencontrol";
import { tool } from "opencontrol/tool";
import { z } from "zod";
import AWS from "aws-sdk";
import { tools } from "sst/opencontrol";

const workout = tool({
  name: "workout_routine",
  description: "Get today's workout routine",
  async run() {
    return "Push day";
  },
});

const aws = tool({
  name: "aws",
  description: "Make a call to the AWS SDK for JavaScript v2",
  args: z.object({
    client: z.string().describe("Class name of the client to use"),
    command: z.string().describe("Command to call on the client"),
    args: z
      .record(z.string(), z.any())
      .optional()
      .describe("Arguments to pass to the command"),
  }),
  async run(input) {
    const client = new AWS[input.client]();
    return await client[input.command](input.args).promise();
  },
});

const app = create({
  key: process.env.OPENCONTROL_KEY,
  tools: [workout, aws, ...tools],
});

export const handler = handle(app);