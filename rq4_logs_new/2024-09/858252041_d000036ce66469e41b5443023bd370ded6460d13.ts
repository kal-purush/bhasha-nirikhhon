import { z } from "zod";
import { commonHeaders } from "../lib/config";

const timestampSchema = z.union([
  z.string().refine((str) => !isNaN(Date.parse(str)), {
    message: "Invalid date string",
  }), // validate if string is a valid date
  z.date(), // allow a Date object
]);

const Task = z.object({
  id: z.number(),
  name: z.string(),
  createdAt: timestampSchema,
  dueDate: timestampSchema,
  done: z.boolean(),
  assignee: z.string(),
});

const TasksResponse = z.object({
  tasks: z.array(Task),
});

export type Task = z.infer<typeof Task>;

export const getTasks = async () => {
  const tasksResponse = await fetch("http://localhost:3000/tasks", {
    method: "get",
    headers: {
      ...commonHeaders,
    },
  });
  const tasksJson = await tasksResponse.json();

  const parsed = TasksResponse.safeParse(tasksJson);
  if (parsed.success) {
    return parsed.data;
  }

  return null;
};