import {
  addTask,
  removeTask,
  tasksReducer,
  TasksState,
  createTask,
} from './tasks-slice';

describe('tasksSlice', () => {
  const initialState: TasksState = {
    entities: [
      createTask({ title: 'Write tests' }),
      createTask({ title: 'Make them pass' }),
    ],
  };

  it(`should add a task when the ${addTask} is fired`, () => {
    const task = createTask({ title: 'Profit' });
    const action = addTask(task);
    const newState = tasksReducer(initialState, action);

    expect(newState.entities).toEqual([task, ...initialState.entities]);
  });

  it(`should remove a task when the ${removeTask} is fired`, () => {
    const task = initialState.entities[0];
    const action = removeTask(task.id);
    const newState = tasksReducer(initialState, action);

    expect(newState).not.toContain(task);
  });
});