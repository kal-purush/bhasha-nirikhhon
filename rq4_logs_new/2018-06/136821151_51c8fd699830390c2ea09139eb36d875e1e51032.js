import React, { Component } from 'react';
import Axios from 'axios';
import moment from 'moment';

const BASE_URL = 'https://api.mvp48.co/todo';

class TodoList extends Component {
  constructor(props) {
    super(props);
    this.state = { tasks: [] };
    this.handleMarkDone = this.toggleDoneFlag.bind(this);
    this.userId = window.location.search.split('=')[1];
  }

  async componentWillMount() {
    await this.loadTasks();
  }

  async loadTasks() {
    const { data } = await Axios.get(`${BASE_URL}?userId=${this.userId}`);
    this.setState({
      tasks: data,
    });
  }

  async toggleDoneFlag(task) {
    await Axios.put(`${BASE_URL}/${task.id}/toggle-done?userId=${this.userId}`);
    await this.loadTasks();
  }

  async toggleImportantFlag(task) {
    await Axios.put(`${BASE_URL}/${task.id}/toggle-important?userId=${this.userId}`);
    await this.loadTasks();
  }

  render() {
    const tasks = this.state.tasks.map(task => (
      <tr
        key={task.id}
        style={{ textDecoration: task.done ? 'line-through' : 'none' }}
      >
        <td>
          <input
            className="form-check-input"
            type="checkbox"
            value=""
            checked={task.done}
            onChange={({ target }) => {
            this.toggleDoneFlag(task, target);
          }}
          />
        </td>
        <td>
          {task.name}
        </td>
        <td>
          {moment(task.dateTime).utc().format('LLL')}
        </td>
        <td>
          <button
            className="btn btn-primary btn-sm"
            onClick={({ target }) => {
              this.toggleImportantFlag(task, target);
            }}
          >
            { task.important ? <span>Pinned</span> : <span>Pin</span> }
          </button>
        </td>
      </tr>
    ));

    return (
      <table className="table table-bordered table-responsive">
        <thead>
          <tr>
            <th />
            <th>task name</th>
            <th>Date Time</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {tasks}
        </tbody>
      </table>
    );
  }
}

export default TodoList;