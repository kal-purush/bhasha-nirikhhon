import { Project } from "../../types/Interface";
import initialTask from "../initials/initialTask";

export default function createNewTask(
  projects: any,
  projectName: any,
  sectionName: any,
  taskName: any
) {
  const newTask = Object.assign({}, initialTask, {
    name: taskName,
  });
  const newState: Project[] = [...projects].map((project) =>
    project.projectName === projectName
      ? {
          ...project,
          sections: project.sections
            ? [...project.sections].map((section) =>
                section.name === sectionName
                  ? { ...section, tasks: section.tasks.concat(newTask) }
                  : section
              )
            : [],
        }
      : project
  );
  return newState;
}