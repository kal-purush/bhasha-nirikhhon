import { Check, Playbook, Session, Step } from "./types";

export default function populateSystemPrompt(
  systemPrompt: string,
  session: Session,
  playbook: Playbook,
  step: Step,
  check: Check
): string {
  let text = systemPrompt;

  text = text.replace(`{{tableName}}`, session.tableName || "");
  text = text.replace(`{{tableDocumentation}}`, session.tableDocumentation || "");
  text = text.replace(`{{tableSql}}`, session.tableSql || "");
  text = text.replace(`{{tableData}}`, JSON.stringify(session.tableData));
  text = text.replace(`{{playbookGoal}}`, playbook.goal || "");
  text = text.replace(`{{stepName}}`, step.name || "");
  text = text.replace(`{{stepDescription}}`, step.description || "");
  text = text.replace(`{{checkName}}`, check.name || "");
  text = text.replace(`{{checkDescription}}`, check.description || "");
  return text;
}