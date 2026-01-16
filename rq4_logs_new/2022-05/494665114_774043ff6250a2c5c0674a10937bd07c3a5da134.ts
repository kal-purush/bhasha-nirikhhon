export type Task = {
  id: string;
  title: string;
  description: string;
  status: "todo" | "progress" | "block" | "qa" | "done" | "deployed";
};

export type Status = "todo" | "progress" | "block" | "qa" | "done" | "deployed";

export enum STATUS {
  todo = "Todo",
  progress = "inProgress",
  block = "Blocked",
  qa = "InQA",
  done = "Done",
  deployed = "Deployed",
}