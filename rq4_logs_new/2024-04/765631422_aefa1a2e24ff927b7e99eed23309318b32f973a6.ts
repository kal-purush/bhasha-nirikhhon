/*
 * This program has been developed by students from the bachelor
 * Computer Science at Utrecht University within the Software Project course.
 *
 * © Copyright Utrecht University
 * (Department of Information and Computing Sciences)
 */

export type ReceiveMessageI = {
  callID: string;
  type: string;
  status: string;
  value: Record<string, any>;
};

export type keyTypeI =
  | "broadcastState"
  | "dbConnection"
  | "schema"
  | "query"
  | "state"
  | "mp-backend";
export type subKeyTypeI =
  // Crud
  | "create"
  | "getAll"
  | "delete"
  | "update"
  | "get"
  | "select"
  // Custom
  | "newDbConnection"
  | "editDbConnection"
  | "deleteDbConnection"
  | "getDbConnection"
  | "getAllDbConnections"
  | "testConnection"
  | "getSchema"
  | "getSchemaStats"
  | "runQuery";

export type SendMessageI = {
  key: keyTypeI;
  subKey?: subKeyTypeI;
  body?: any;
};

export type SendMessageWithSessionI = SendMessageI & {
  callID: string;
  sessionID: string;
  body?: string;
};

export type QueuedMessage = {
  message: SendMessageI;
  callback?: Function;
};