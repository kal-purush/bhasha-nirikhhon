import { object, string, boolean, array } from "yup";

export let taskSchema = object({
  title: string().required().min(3).max(50),
  description: string().required().min(3).max(500),
  status: string().required().oneOf(["TODO", "DOING", "DONE"]),
  subtasks: array().of(
    object().shape({
      title: string().required().min(3).max(50),
    })
  ),
});

export let boardSchema = object({
  name: string().required().min(3).max(50),
  columns: array().of(
    object().shape({
      name: string().required().oneOf(["TODO", "DOING", "DONE"]),
    })
  ),
});