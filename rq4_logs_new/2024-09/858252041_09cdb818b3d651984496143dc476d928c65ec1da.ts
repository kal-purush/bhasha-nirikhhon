import { z } from "zod";
import type { ENVIRONMENT } from "../../../lib/config";
import { getTracingHeader, getAuthHeader, config } from "../../../lib/config";
import { User } from "../../userInfo/getUser";

const UsersResponse = z.object({ users: z.array(User) });

export const getUsers = async (environment: ENVIRONMENT) => {
  const usersResponse = await fetch(`${config[environment].baseUrl}/users`, {
    method: "get",
    headers: {
      "Content-Type": "application/json",
      ...getTracingHeader(),
      ...getAuthHeader(),
    },
  });
  const usersJson = await usersResponse.json();

  const parsed = UsersResponse.safeParse(usersJson);
  if (parsed.success) {
    return parsed.data;
  }

  return null;
};