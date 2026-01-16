import { callApi } from "../api/callApi";
import { BoardType, ColumnType, SubTaskType, TaskType } from "../types";
import { ENDPOINT, HTTP_METHOD } from "../utils/constants";
//consider keeping the id in the interface
interface BoardApi
  extends Omit<BoardType, "created_at" | "updated_at" | "columns" | "id"> {}

interface ColumnApi
  extends Omit<ColumnType, "created_at" | "updated_at" | "tasks" | "id"> {}

interface TaskApi
  extends Omit<TaskType, "created_at" | "updated_at" | "subtasks" | "id"> {}

interface SubTaskApi
  extends Omit<SubTaskType, "created_at" | "updated_at" | "id"> {}

export const getAll = async (endpoint: ENDPOINT) => {
  const response = await callApi(HTTP_METHOD.GET, endpoint);
  return response.data;
};

export const getOne = async (endpoint: ENDPOINT, id: string) => {
  const response = await callApi(HTTP_METHOD.GET, `${endpoint}/${id}`);
  return response.data;
};
export const create = async (
  endpoint: ENDPOINT,
  data: BoardApi | ColumnApi | TaskApi | SubTaskApi
) => {
  const response = await callApi(HTTP_METHOD.POST, endpoint, data);
  return response.data;
};

export const remove = async (endpoint: ENDPOINT, id: string) => {
  const response = await callApi(HTTP_METHOD.DELETE, `${endpoint}/${id}`);
  return response.data;
};

export const update = async (
  endpoint: ENDPOINT,
  id: string,
  data: BoardApi | ColumnApi | TaskApi | SubTaskApi
) => {
  const response = await callApi(HTTP_METHOD.PUT, `${endpoint}/${id}`, data);
  return response.data;
};