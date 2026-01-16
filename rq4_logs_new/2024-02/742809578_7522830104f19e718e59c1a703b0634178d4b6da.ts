export type TTask = {
  title: string;
  subtitle: string;
  tasks: Array<{ taskTitle: string; taskDescription: string; isComplete: boolean }>;
};