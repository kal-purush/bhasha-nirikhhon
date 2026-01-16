const types = {
  ADD: '[TODO] Add',
  ADD_FAIL: '[TODO] Add Fail',
  ADD_SUCCESS: '[TODO] Add Success',
  REMOVE: '[TODO] Remove',
  REMOVE_FAIL: '[TODO] Remove',
  REMOVE_SUCCESS: '[TODO] Remove',
  LOAD: '[TODO] Load',
  LOAD_FAIL: '[TODO] Load Fail',
  LOAD_SUCCESS: '[TODO] Load Success'
};

const addSuccess = (state, todo) => ({
  ids: [...state.ids, todo.id],
  entities: [...state.entities, todo],
  selectedId: state.selectedId
});

const loadSuccess = (state, todos) => ({
  ids: todos.map(todo => todo.id),
  entities: todos,
  selectedId: null
});

export const todo = {
  actions: {
    addSuccess,
    loadSuccess
  },
  types
};