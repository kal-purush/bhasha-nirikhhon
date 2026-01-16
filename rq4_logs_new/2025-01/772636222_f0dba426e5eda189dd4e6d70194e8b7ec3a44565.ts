import { httpErrors } from "@fastify/sensible";
import { MessageToDeliver } from "../../types/messages.js";
import { BuildingBlocksSDK } from "@ogcio/building-blocks-sdk";

export function prepareForSecureDelivery(
  profile: Awaited<
    ReturnType<BuildingBlocksSDK["profile"]["getProfile"]>
  >["data"],
  messageToDeliver: MessageToDeliver,
): MessageToDeliver & { transports: string[] } {
  if (messageToDeliver.securityLevel === "public") {
    return {
      ...messageToDeliver,
      transports: messageToDeliver.transports ?? [],
    };
  }

  const cloned = { ...messageToDeliver };

  cloned.body = getSecureBody(profile, messageToDeliver);
  cloned.attachmentIds = undefined;

  return { ...cloned, transports: cloned.transports ?? [] };
}

function getSecureBody(
  user: Awaited<ReturnType<BuildingBlocksSDK["profile"]["getProfile"]>>["data"],
  message: MessageToDeliver,
) {
  if (!process.env.MESSAGING_SHOW_MESSAGE_URL) {
    throw httpErrors.internalServerError(
      "Missing MESSAGING_SHOW_MESSAGE_URL variable",
    );
  }
  const seeMessageUrl = process.env.MESSAGING_SHOW_MESSAGE_URL.replace(
    "{{language}}",
    user.preferredLanguage,
  ).replace("{{messageId}}", message.id);
  return `Dear ${user.publicName},

You have a new secure message.

The sender has flag it as being confidential. It's held on the Gov.IE secure messaging system. You will have to login with  a verified MyGovId account to access the message.

[Click here to Access Message][1]

if the above link doesn't work, please copy paste the following url in your browser
${seeMessageUrl}

Thanks
Gov.Ie team

[1]: ${seeMessageUrl}`;
}