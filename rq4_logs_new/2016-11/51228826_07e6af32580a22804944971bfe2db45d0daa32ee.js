import React, { PropTypes } from 'react';
import { connect } from 'react-redux';
import * as actions from '../action/counter';

const style = {
  textAlign: 'center',
  paddingTop: 200,
};

export const App = (props) => (
  <div style={style}>
    <div>{props.counter}</div>
    <button onClick={props.increment}>+</button>
    {' '}
    <button onClick={props.decrement}>-</button>
    {' '}
    <button onClick={props.delayIncrement}>delay</button>
  </div>
);
App.propTypes = {
  counter: PropTypes.number.isRequired,
  increment: PropTypes.func.isRequired,
  decrement: PropTypes.func.isRequired,
};

export default connect(
  state => ({counter: state.counter}),
  dispatch => ({
    increment: () => dispatch(actions.increment()),
    decrement: () => dispatch(actions.decrement()),
    delayIncrement: () => dispatch(actions.delayIncrement())
  })
)(App);