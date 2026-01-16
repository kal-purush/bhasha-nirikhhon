import { Project } from '../../types/Interface';
import createProject from './createProject';
import createTask from './createTask';

test('create task', () => {    
    const projectName = "Project A"
    const sectionName = "Backend development";
    const taskName = "New task";
    const projects: Project[] = [];

    const project = createProject(projectName);
    projects.push(project);
    
    const result = createTask(projects, projectName, sectionName, taskName);

    const receivedProject = result[0];
    const receivedSection = receivedProject.sections![1];
    const receivedTask = receivedSection.tasks[0];

    expect(receivedProject.projectName).toBe(projectName);
    expect(receivedSection.name).toBe(sectionName);
    expect(receivedTask.name).toBe(taskName);
  });