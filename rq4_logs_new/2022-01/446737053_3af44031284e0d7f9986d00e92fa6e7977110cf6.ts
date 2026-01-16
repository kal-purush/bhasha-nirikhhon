import axios from "axios";
import { EndpointToInterface } from "./types";
import { createInternalError, sha256 } from "./utils";

const BASE_URL = "https://www.quantconnect.com/api/";

export type QuantConnectConfig = {
  userId: string;
  token: string;
  version?: string;
};

const supportedVersions = ["v2"];

const defaultConfig = {
  version: "v2",
};

type CreateAPIMethod = <T extends keyof EndpointToInterface>(
  endpoint: T
) => EndpointToInterface[T]["paramsRequired"] extends true
  ? (
      params: EndpointToInterface[T]["params"]
    ) => Promise<EndpointToInterface[T]["response"]>
  : (
      params?: EndpointToInterface[T]["params"]
    ) => Promise<EndpointToInterface[T]["response"]>;

const quantconnect = (config: QuantConnectConfig) => {
  const { userId, token, version } = { ...defaultConfig, ...config };
  if (!userId || !token) {
    throw createInternalError(`userId and token are required parameters!`);
  }

  if (!version || !supportedVersions.includes(version)) {
    throw createInternalError(
      `Invalid version used, supported versions are: ${supportedVersions}. Got: ${version}!`
    );
  }

  const createApiMethod: CreateAPIMethod =
    (endpoint) => async (params: any) => {
      const timestamp = new Date().getTime();
      const hash = await sha256(`${token}:${timestamp}`);
      const { data } = await axios(endpoint, {
        baseURL: `${BASE_URL}/${version}`,
        headers: { Timestamp: timestamp.toString() },
        auth: { username: userId, password: hash },
        params,
      });
      return data;
    };

  return {
    authenticate: createApiMethod("authenticate"),
    live: {
      read: createApiMethod("live/read"),
    },
    files: {
      create: createApiMethod("files/create"),
      read: createApiMethod("files/read"),
      update: createApiMethod("files/update"),
      delete: createApiMethod("files/delete"),
    },
    projects: {
      create: createApiMethod("projects/create"),
      read: createApiMethod("projects/read"),
      update: createApiMethod("projects/update"),
      delete: createApiMethod("projects/delete"),
    },
  };
};

export default quantconnect;