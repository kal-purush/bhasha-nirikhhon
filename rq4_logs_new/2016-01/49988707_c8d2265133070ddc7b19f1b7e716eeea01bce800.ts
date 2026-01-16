import {combineReducers} from 'redux';
import consul from './consul.reducer';
import docker from './docker.reducer';

const rootReducer = combineReducers({
  consul,
  docker
});

export default rootReducer;