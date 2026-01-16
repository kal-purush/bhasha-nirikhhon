import dotenv from "dotenv";

// Parsing the env file.
dotenv.config();

interface IConfig {
  serverHost: string;
  apiKey: string;
  userName: string;
}

export const getWsServerUrl = (c: IConfig): string => {
  const socketPath = "/socket";
  return (
    "ws://" +
    c.serverHost +
    socketPath +
    "?key=" +
    c.apiKey +
    "&name=" +
    c.userName
  );
};

export const loadConfig = (): IConfig => {
  return {
    serverHost: process.env.SERVER_HOST || "localhost:8080",
    apiKey: process.env.USER_API_KEY || "webuild",
    userName: process.env.USER_NAME || "noname",
  };
};