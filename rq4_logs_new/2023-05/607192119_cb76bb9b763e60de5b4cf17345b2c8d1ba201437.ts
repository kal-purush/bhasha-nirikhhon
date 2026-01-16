import axios, { type AxiosInstance } from "axios";
import type { User, Channel, Member } from "./types";

export const getClient = (graphExplorerAccessToken: string) => {
  return axios.create({
    baseURL: "https://graph.microsoft.com/beta/",
    headers: {
      Authorization: `Bearer ${graphExplorerAccessToken}`,
      "Content-Type": "application/json",
    },
  });
};

const request = async <Response>(
  client: AxiosInstance,
  url?: string,
  result: Response[] = [],
  length = -1
): Promise<Response[]> => {
  if (!url || url === "") return result;

  const res = await client.get<{
    "@odata.context": string;
    "@odata.count": number;
    "@odata.nextLink": string;
    value: Response[];
  }>(url, {
    headers: {
      ConsistencyLevel: "eventual",
    },
  });

  result.push(...res.data.value);

  if (length === -1) length = res.data["@odata.count"];
  console.log(`${result.length} / ${length}`);

  if (res.data.value.length !== 0) {
    return request(client, res.data["@odata.nextLink"], result, length);
  } else {
    return result;
  }
};

export const getAllChannels = async (client: AxiosInstance, teamId: string) => {
  const GROUP_ORDER_PREFIXS = ["部活", "IWテーマ", "授業"];

  const res = await client.get<{ value: Channel[] }>(
    `teams/${teamId}/channels`
  );
  const channels = res.data.value
    .filter(({ displayName }) => displayName !== "General")
    .map(({ id, displayName, membershipType }) => ({
      id,
      displayName,
      type: membershipType,
    }))
    .sort((a, b) => {
      if (a.type === "standard" && b.type === "private") return 1;
      if (a.type === "private" && b.type === "standard") return -1;

      const aIndex = GROUP_ORDER_PREFIXS.findIndex((groupPrefix) =>
        a.displayName.startsWith(groupPrefix)
      );
      const bIndex = GROUP_ORDER_PREFIXS.findIndex((groupPrefix) =>
        b.displayName.startsWith(groupPrefix)
      );
      if (aIndex > bIndex) return 1;
      if (aIndex < bIndex) return -1;

      return a.displayName.localeCompare(b.displayName, "ja");
    });

  return channels;
};

export const getChannelMembers = async (
  client: AxiosInstance,
  teamId: string,
  channelId: string
) => {
  const res = await client.get<{ value: Member[] }>(
    `teams/${teamId}/channels/${channelId}/members`
  );
  const members = res.data.value.map((user) => user.email.toLowerCase());
  return members;
};

export const getTeamMembers = async (client: AxiosInstance, teamId: string) => {
  const members = await request<Member>(client, `teams/${teamId}/members`);
  return members!;
};

export const getUsers = async (client: AxiosInstance, emailDomain: string) => {
  if (!emailDomain.startsWith("@")) {
    throw new Error("`emailDomain` must start with @");
  }
  const users = await request<User>(
    client,
    `/users?$filter=endswith(mail,'${emailDomain}')&$orderby=userPrincipalName&$count=true`
  );
  return users;
};

export const inviteUserToChannel = async (
  client: AxiosInstance,
  teamId: string,
  channelId: string,
  userEmail: string,
  domain: string,
  isOwner: boolean
) => {
  if (!userEmail.endsWith(domain)) {
    throw new Error(`\`userEmail\` must end with ${domain}`);
  }

  const res = await client.post(
    `teams/${teamId}/channels/${channelId}/members`,
    {
      "@odata.type": "#microsoft.graph.aadUserConversationMember",
      roles: isOwner ? ["owner"] : [],
      "user@odata.bind": `https://graph.microsoft.com/beta/users('${userEmail}')`,
    }
  );
  return res;
};

export const inviteUserToTeam = async ({
  teamId,
  userIds,
  generalChannelId,
  teamsAuthToken,
  teamsSkypeTokenAsm,
  isOwner,
}: {
  teamId: string;
  userIds: string[];
  generalChannelId: string;
  teamsAuthToken: string;
  teamsSkypeTokenAsm: string;
  isOwner: boolean;
}) => {
  const payload = {
    users: userIds.map((userId) => ({
      mri: `8:orgid:${userId}`,
      role: Number(isOwner),
    })),
    groupId: teamId,
  };

  const res = await axios.put(
    `https://teams.microsoft.com/api/mt/apac/beta/teams/${generalChannelId}/bulkUpdateRoledMembers?allowBotsInChannel=true`,
    payload,
    {
      headers: {
        authorization: `Bearer ${teamsAuthToken}`,
        "x-skypetoken": teamsSkypeTokenAsm,
      },
    }
  );

  return res;
};