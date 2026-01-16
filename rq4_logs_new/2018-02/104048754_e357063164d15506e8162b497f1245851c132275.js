import ${DATA_CLASS_NAME} from './data/${DATA_CLASS_NAME}.js'

function getInitialState() {
  return new ${DATA_CLASS_NAME}();
}

const defaultState = getInitialState();

export default (state = defaultState, action) => {
  switch (action.type) {
    default:
      return state;
  }
};