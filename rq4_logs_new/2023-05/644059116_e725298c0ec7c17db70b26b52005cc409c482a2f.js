import React, { Fragment } from "react";
import { useDispatch } from "react-redux";
import { filterActions } from "../../store/filter-slice";

const Filters = (props) => {
  const dispatch = useDispatch();
  const totalTask = props.totalTask.length;
  const clearCompletedHandler = () => {
    dispatch(filterActions.deleteAllCompletedTask());
  };

  const getAllTasksHandler = () => {
    props.onAll()
  };

  const getActiveTasksHandler = () => {
    props.onActive()
  };

  const getCompletedTasksHandler = () => {
    props.onCompleted()
  };

  return (
    <Fragment>
      <div className="filterButtonsDesktop">
        <p>
          {totalTask} {totalTask <= 1 ? "item left" : "items left"}
        </p>
        <div>
          <span onClick={getAllTasksHandler}>All</span>
          <span onClick={getActiveTasksHandler}>Active</span>
          <span onClick={getCompletedTasksHandler}>Completed</span>
        </div>
        <span className="clearCompleted" onClick={clearCompletedHandler}>Clear Completed</span>
      </div>

      <div className="filterButtons">
        <span onClick={getAllTasksHandler}>All</span>
        <span onClick={getActiveTasksHandler}>Active</span>
        <span onClick={getCompletedTasksHandler}>Completed</span>
      </div>
    </Fragment>
  );
};

export default Filters;