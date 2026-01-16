import * as actions from 'common/data/todo/actions';
import { TodoActionTypes, Todo } from 'common/data/todo/types';

describe('todo actions', () => {
  describe('add todo actions', () => {
    it('should create an action request to add a todo', () => {
      const todo: Todo = {
        id: 'test_id',
        createdAt: 0,
        updatedAt: 0,
        detail: 'todo details',
        complete: false
      };
      const expectedAction = {
        type: TodoActionTypes.CREATE_REQUEST,
        payload: todo
      };
  
      expect(actions.createRequest(todo)).toEqual(expectedAction);
    });

    it('should create an action success to add a todo', () => {
      const todo: Todo = {
        id: 'test_id',
        createdAt: 0,
        updatedAt: 0,
        detail: 'todo details',
        complete: false
      };
      const expectedAction = {
        type: TodoActionTypes.CREATE_SUCCESS,
        payload: todo
      };
  
      expect(actions.createSuccess(todo)).toEqual(expectedAction);
    });

    it('should create an action error to add a todo', () => {
      const errorMessage = 'This is an error message';
      
      const expectedAction = {
        type: TodoActionTypes.CREATE_ERROR,
        payload: errorMessage
      };
  
      expect(actions.createError(errorMessage)).toEqual(expectedAction);
    });
  });

  describe('update todo actions', () => {
    it('should create an action request to update a todo', () => {
      const todo: Todo = {
        id: 'test_id',
        createdAt: 0,
        updatedAt: 0,
        detail: 'todo details',
        complete: true
      };
      const expectedAction = {
        type: TodoActionTypes.UPDATE_REQUEST,
        payload: todo
      };

      expect(actions.updateRequest(todo)).toEqual(expectedAction);
    });

    it('should create an action success to update a todo', () => {
      const todo: Todo = {
        id: 'test_id',
        createdAt: 0,
        updatedAt: 0,
        detail: 'todo details',
        complete: true
      };
      const expectedAction = {
        type: TodoActionTypes.UPDATE_SUCCESS,
        payload: todo
      };

      expect(actions.updateSuccess(todo)).toEqual(expectedAction);
    });

    it('should create an action error to update a todo', () => {
      const errorMessage = 'This is an error message';
      const expectedAction = {
        type: TodoActionTypes.UPDATE_ERROR,
        payload: errorMessage
      };

      expect(actions.updateError(errorMessage)).toEqual(expectedAction);
    });
  });

  describe('delete all todo actions', () => {
    it('should create an action request to clear all todos', () => {
      const expectedAction = {
        type: TodoActionTypes.DELETE_ALL_REQUEST
      };
      
      expect(actions.deleteAllRequest()).toEqual(expectedAction);
    });

    it('should create an action success to clear all todos', () => {
      const expectedAction = {
        type: TodoActionTypes.DELETE_ALL_SUCCESS
      };
      
      expect(actions.deleteAllSuccess()).toEqual(expectedAction);
    });
  });

  describe('fetch all todo actions', () => {
    it('should create an action request to fetch all todos', () => {
      const expectedAction = {
        type: TodoActionTypes.FETCH_ALL_REQUEST
      }

      expect(actions.fetchAllRequest()).toEqual(expectedAction);
    });

    it('should create an action success to fetch all todos', () => {
      const expectedAction = {
        type: TodoActionTypes.FETCH_ALL_SUCCESS
      }

      expect(actions.fetchAllSuccess()).toEqual(expectedAction);
    });

    it('should create an action request to fetch all todos', () => {
      const errorMessage = 'This is an error message';
      const expectedAction = {
        type: TodoActionTypes.FETCH_ALL_ERROR,
        payload: errorMessage
      }

      expect(actions.fetchAllError(errorMessage)).toEqual(expectedAction);
    });
  });

  describe('fetch todo actions', () => {
    it('should create an action to fetch a todo', () => {
      const todoId = 'test_id';
      const expectedAction = {
        type: TodoActionTypes.FETCH_REQUEST,
        payload: todoId
      }

      expect(actions.fetchRequest(todoId)).toEqual(expectedAction);
    });

    it('should create an action success to fetch a todo', () => {
      const todo: Todo = {
        id: 'test_id',
        createdAt: 0,
        updatedAt: 0,
        detail: 'todo details',
        complete: false
      };
      const expectedAction = {
        type: TodoActionTypes.FETCH_SUCCESS,
        payload: todo
      }

      expect(actions.fetchSuccess(todo)).toEqual(expectedAction);
    });

    it('should create an action error to fetch a todo', () => {
      const errorMessage = 'This is an error message';
      const expectedAction = {
        type: TodoActionTypes.FETCH_ERROR,
        payload: errorMessage
      }

      expect(actions.fetchError(errorMessage)).toEqual(expectedAction);
    });
  });
});