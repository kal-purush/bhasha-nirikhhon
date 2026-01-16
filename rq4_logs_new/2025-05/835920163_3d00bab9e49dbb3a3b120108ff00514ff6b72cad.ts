import { Session } from "./types";

export default function parsePlaybookYaml(
  session: Session,
  stepName: string,
  checkName: string
): string {
  const step = session.steps.find((s) => s.key === stepName);
  const check = step?.checks.find((c) => c.key === checkName);
  console.log(stepName, checkName, session, check);
  if (!check) {
    return "";
  }
  if (check.assertion) {
    return "FINISHED";
  }
  const lastMessage = check.messages[check.messages.length - 1];
  if (
    lastMessage.role === "assistant" &&
    lastMessage.content &&
    (lastMessage.content.trim().startsWith("ASSERTION") || 
      lastMessage.content.trim().startsWith("ASK_USER"))
  ) {
    return "USER_ACTION";
  } 
  return "UNDERWAY";
}