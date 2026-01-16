import projectReducer from "./projectReducer";
import {
  clearProjects,
  addProject,
  delProject,
  addTask,
  delTask,
  addSubtask,
} from "./projectReducer";
import initialProject from "../initials/initialProject";
import initialState from "../initials/initialState";
import { Role, Task, Type } from "../../types/Interface";
import state_with_one_project from "./projectReducerTestUtils";
import testUtils from "./projectReducerTestUtils";


describe('testing store actions related to the project', () => {
  test('add new project', () => {
    const previousState = initialState;
    const title = "New Project Title";
    const id = "123";
    const newState = projectReducer(previousState, addProject({projectName: title, projectId: id}));
    expect(newState.projects.length).toEqual(1);
    expect(newState.projects[0].projectName).toEqual(title);
    expect(newState.projects[0].projectId).toEqual(id);
  });

  test('add second project', () => {
    const previousState = initialState;
    const title1 = "New Project Title 1";
    const title2 = "New Project Title 2";
    const id1 = "123";
    const id2 = "456";
  
    let newState = projectReducer(previousState, addProject({projectName: title1, projectId: id1}));
    newState = projectReducer(newState, addProject({projectName: title2, projectId: id2}));
  
    expect(newState.projects.length).toEqual(2);
    expect(newState.projects[0].projectName).toEqual(title1);
    expect(newState.projects[1].projectName).toEqual(title2);
    expect(newState.projects[0].projectId).toEqual(id1);
    expect(newState.projects[1].projectId).toEqual(id2);
  });
  
  test('add project having the same name', () => {
    const previousState = initialState;
    const title = "New Project Title";
    const id = "123";
    let newState = projectReducer(previousState, addProject({projectName: title, projectId: id}));
    
    const consoleSpy = jest.spyOn(console, 'log');
  
    try {
      newState = projectReducer(newState, addProject({projectName: title, projectId: id}));
    }
    // version for handing errors
    // catch(error) {
    //   expect(error).toBeInstanceOf(TypeError);
    //   expect(error).toHaveProperty('message', 'Something bad happened!');
    // }
    // version for console log
    catch(consoleSpy) {
      expect(consoleSpy).toHaveBeenCalledWith('error');
      }
    expect(newState.projects.length).toEqual(1);
  });
  
  test('delete project', () => {
    const previousState = testUtils.stateWithEmptyProject;
  
    const projectId = "1b3cada1-6c61-4974-9886-d3651f76007d";
    const newState = projectReducer(previousState, delProject({projectId: projectId}));
  
    const expectedState = {"projects": []};
    expect(newState).toEqual(expectedState);
  });

  test('clear projects', () => {
    const previousState = testUtils.stateWithTwoProjects;

    const newState = projectReducer(previousState, clearProjects());
  
    const expectedState = {"projects": []};
    expect(newState).toEqual(expectedState);
  });

});

describe('testing store actions related to tasks, group and subtasks', () => {
  test('add new task', () => {
    const previousState = testUtils.stateWithEmptyProject;
  
    const projectId = "1b3cada1-6c61-4974-9886-d3651f76007d";
    const sectionName = "Backend development";
    const taskName = "New task";
    const type = "task";
    const newState = projectReducer(previousState, addTask({projectId: projectId, sectionName: sectionName, taskName: taskName, type: type}));
  
    const receivedSections = newState.projects[0].sections!;
    expect(receivedSections[1].tasks[0].name).toEqual(taskName);
    expect(receivedSections[1].tasks[0].minMd).toEqual(0);
    expect(receivedSections[1].tasks[0].maxMd).toEqual(0);
    expect(receivedSections[1].tasks[0].predictedMd).toEqual(0);
    expect(receivedSections[1].tasks[0].risk).toEqual("L");
    expect(receivedSections[1].tasks[0].role).toEqual(Role.BD);
  });
  
  test('delete task', () => {
    const previousState = testUtils.stateWithEmptyProject;
  
    // const taskId = "f819c1a6-5237-44b5-a4fe-623f911c2a8c";
    const sectionName = "Backend development";
    const projectId = "1b3cada1-6c61-4974-9886-d3651f76007d";
    const taskName = "New task";
    const type = "task";

    const afterAddingTaskState = projectReducer(previousState, addTask({projectId: projectId, sectionName: sectionName, taskName: taskName, type: type}));

    const receivedSections = afterAddingTaskState.projects[0].sections!;
    const taskId = receivedSections[1].tasks[0].id;

    const newState = projectReducer(previousState, delTask({projectId: projectId, sectionName: sectionName, id: taskId}));  
    const expectedState = previousState;

    expect(newState).toEqual(expectedState);  
  });

  test('add new group', () => {

    const previousState = testUtils.stateWithEmptyProject;
  
    const projectId = "1b3cada1-6c61-4974-9886-d3651f76007d";
    const sectionName = "Backend development";
    const taskName = "New group";
    const type = "group";
    const newState = projectReducer(previousState, addTask({projectId: projectId, sectionName: sectionName, taskName: taskName, type: type}));
  
    const receivedSections = newState.projects[0].sections!;
    expect(receivedSections[1].tasks[0].name).toEqual(taskName);
    expect(receivedSections[1].tasks[0].minMd).toEqual(null);
    expect(receivedSections[1].tasks[0].maxMd).toEqual(null);
    expect(receivedSections[1].tasks[0].predictedMd).toEqual(null);
    expect(receivedSections[1].tasks[0].risk).toEqual("");
    expect(receivedSections[1].tasks[0].role).toEqual(Role.BD);
  });

  test('add new subtask to group', () => {
    const previousState = testUtils.stateWithEmptyProject;
  
    const projectId = "1b3cada1-6c61-4974-9886-d3651f76007d";
    const sectionName = "Backend development";
    const taskName = "New group";
    const subtaskName = "New subtask";
    const type = Type.Group;
    const afterAddingTaskState = projectReducer(previousState, 
      addTask({projectId: projectId, sectionName: sectionName, taskName: taskName, type: type}));
    
    const receivedSectionsAfterAddingTask = afterAddingTaskState.projects[0].sections!;
    const receivedTaskID = receivedSectionsAfterAddingTask[1].tasks[0].id;

    const afterAddingSubtaskState = projectReducer(afterAddingTaskState, 
      addSubtask({projectId: projectId, sectionName: sectionName, taskId: receivedTaskID, subtaskName: subtaskName}));
  
    const receivedSectionsAfterAddingSubtask = afterAddingSubtaskState.projects[0].sections!;
    const receivedSubtasksList = receivedSectionsAfterAddingSubtask[1].tasks[0].subtasks!;

    expect(receivedSubtasksList.length).toEqual(1);
    expect(receivedSubtasksList[0].name).toEqual(subtaskName);
    expect(receivedSubtasksList[0].minMd).toEqual(0);
    expect(receivedSubtasksList[0].maxMd).toEqual(0);
    expect(receivedSubtasksList[0].predictedMd).toEqual(0);
    expect(receivedSubtasksList[0].risk).toEqual("L");
    expect(receivedSubtasksList[0].role).toEqual(Role.BD);
  });

  test('add second subtask to group', () => {
    const previousState = testUtils.stateWithEmptyProject;
  
    const projectId = "1b3cada1-6c61-4974-9886-d3651f76007d";
    const sectionName = "Backend development";
    const taskName = "New group";
    const subtaskName = "New subtask";
    const secondSubtaskName = "Second subtask";
    const type = Type.Group;
    const afterAddingTaskState = projectReducer(previousState, 
      addTask({projectId: projectId, sectionName: sectionName, taskName: taskName, type: type}));
    
    const receivedSectionsAfterAddingTask = afterAddingTaskState.projects[0].sections!;
    const receivedTaskID = receivedSectionsAfterAddingTask[1].tasks[0].id;

    const afterAddingSubtaskState = projectReducer(afterAddingTaskState, 
      addSubtask({projectId: projectId, sectionName: sectionName, taskId: receivedTaskID, subtaskName: subtaskName}));

    const afterAddingSecondSubtaskState = projectReducer(afterAddingSubtaskState, 
      addSubtask({projectId: projectId, sectionName: sectionName, taskId: receivedTaskID, subtaskName: secondSubtaskName}));
  
    const receivedSectionsAfterAddingSecondSubtask = afterAddingSecondSubtaskState.projects[0].sections!;
    const receivedSubtasksList = receivedSectionsAfterAddingSecondSubtask[1].tasks[0].subtasks!;

    // subtasks list lenght should be equal 2
    expect(receivedSubtasksList.length).toEqual(2);

    // expections about first subtask
    expect(receivedSubtasksList[0].name).toEqual(subtaskName);

    // expections about second subtask
    expect(receivedSubtasksList[1].name).toEqual(secondSubtaskName);
    expect(receivedSubtasksList[1].minMd).toEqual(0);
    expect(receivedSubtasksList[1].maxMd).toEqual(0);
    expect(receivedSubtasksList[1].predictedMd).toEqual(0);
    expect(receivedSubtasksList[1].risk).toEqual("L");
    expect(receivedSubtasksList[1].role).toEqual(Role.BD);
  });

  test('add new subtask to task - it should be not allowed', () => {
    const previousState = testUtils.stateWithEmptyProject;
  
    const projectId = "1b3cada1-6c61-4974-9886-d3651f76007d";
    const sectionName = "Backend development";
    const taskName = "New task";
    const subtaskName = "New subtask";
    const type = Type.Task;
    const afterAddingTaskState = projectReducer(previousState, 
      addTask({projectId: projectId, sectionName: sectionName, taskName: taskName, type: type}));
    
    const receivedSectionsAfterAddingTask = afterAddingTaskState.projects[0].sections!;
    const receivedTaskID = receivedSectionsAfterAddingTask[1].tasks[0].id;

    const afterAddingSubtaskState = projectReducer(afterAddingTaskState, 
      addSubtask({projectId: projectId, sectionName: sectionName, taskId: receivedTaskID, subtaskName: subtaskName}));
  
    const receivedSectionsAfterAddingSubtask = afterAddingSubtaskState.projects[0].sections!;
    const receivedSubtasksList = receivedSectionsAfterAddingSubtask[1].tasks[0].subtasks!;

    // subtasks list should be empty []
    expect(receivedSubtasksList.length).toEqual(0);
  });

});





