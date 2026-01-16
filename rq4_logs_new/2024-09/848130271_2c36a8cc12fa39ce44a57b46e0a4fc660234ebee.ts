import config from "./config";

const LogConfig: { [key: string]: any } = {
  development: {
    transport: {
      targets: [
        {
          target: "pino-pretty",
          level: config.LOG_LEVEL,
          options: {
            translateTime: "HH:MM:ss Z",
          },
        },
      ],
    },
  },
  production: {
    transport: {
      targets: [
        {
          target: "pino-pretty",
          level: config.LOG_LEVEL,
          options: {
            translateTime: "HH:MM:ss Z",
            ignore: "pid,hostname",
          },
        },
        {
          target: "pino/file",
          level: config.FILE_LOG_LEVEL,
          options: { destination: "./logs" }, // this writes to STDOUT
        },
      ],
    },
  },
};
export default LogConfig[config.NODE_ENV];