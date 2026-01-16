import { middyfy } from "../../libs/lambda";

export async function basicAuthorizer(event, _ctx, cb) {
  if (event["type"] != "TOKEN") cb("Unauthorized");

  try {
    const { authorizationToken } = event;
    const encodedCreds = authorizationToken.split(" ")[1];
    const buff = Buffer.from(encodedCreds, "base64");
    const decodedCreds = buff.toString("utf-8").split(":");
    const username = decodedCreds[0];
    const password = decodedCreds[1];

    const storedPassword = process.env[username];
    const effect =
      !storedPassword || storedPassword !== password ? "Deny" : "Allow";
    const policy = generatePolicy(encodedCreds, event.methodArn, effect);
    cb(null, policy);
  } catch (error) {
    cb(`Unauthorized: ${error.message}`);
  }
}

function generatePolicy(principalId, resource, effect = "Allow") {
  return {
    principalId: principalId,
    policyDocument: {
      Version: "2012-10-17",
      Statement: [
        {
          Action: "execute-api:Invoke",
          Effect: effect,
          Resource: resource,
        },
      ],
    },
  };
}

export const main = middyfy(basicAuthorizer);