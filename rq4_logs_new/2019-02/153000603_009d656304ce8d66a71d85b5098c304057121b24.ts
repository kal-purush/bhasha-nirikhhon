import * as selectors from 'common/data/todo/selectors';
import { ApplicationState } from 'common/store';
import assert from 'assert';

describe('todo selectors', () => {
  const todo = {
    id: 'test_id',
    createdAt: 0,
    updatedAt: 0,
    detail: '',
    complete: false
  };
  const state: ApplicationState = {
    todo: {
      loading: false,
      error: undefined,
      byId: {
        [todo.id]: todo
      },
      allIds: ['test_id']
    },
    form: {}
  };

  describe('all todo ids by descending date', () => {
    it('should return id array', () => {
      assert.deepEqual(selectors.selectAllTodoByDescDate(state), ['test_id']);
    });

    it('executes memoization once', () => {
        assert.deepEqual(selectors.selectAllTodoByDescDate(state), ['test_id']);
        assert.equal(selectors.selectAllTodoByDescDate.recomputations(), 1);
    });
  });

  describe('a todo by id', () => {
    it('should return a todo', () => {
      const getTodoById = selectors.selectTodoById();
      assert.deepEqual(getTodoById(state, {id: todo.id}), todo);
    });

    it('executes memoization once', () => {
      const getTodoById = selectors.selectTodoById();
      assert.deepEqual(getTodoById(state, {id: todo.id}), todo);
      assert.equal(selectors.selectAllTodoByDescDate.recomputations(), 1);
    });
  });
});