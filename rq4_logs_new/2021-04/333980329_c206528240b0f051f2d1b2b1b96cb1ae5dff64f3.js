import React, { Component } from "react";
import "./Tasks.css";

class Tasks extends Component {
  constructor() {
    super();
    // state variables
    this.state = {
      tasks: [],
      value: "",
    };
    this.handleChange = this.handleChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);
    this.handleDelete = this.handleDelete.bind(this);
    this.addTask = this.addTask.bind(this);
    this.removeTask = this.removeTask.bind(this);
    this.updateHelp = this.updateHelp.bind(this);

    // Query Selector variables
    // this.root = document.querySelector("#taskList");
    this.form = document.querySelector("#taskForm");
    this.input = document.querySelector("#taskInput");
    this.help = document.querySelector("#taskHelp");
    this.ul = document.querySelector("#taskul");
    this.items = {}; // id -> li element
  }

  // Event Handlers
  // if (document.body.contains(document.getElementById("taskForm"))) {
  handleChange = (e) => {
    this.setState({ value: e.target.value });
  };

  handleSubmit = (e) => {
    e.preventDefault();
    const task = this.state.value;
    this.addTask(task);
    // this.input.value = "";
  };

  handleDelete = (e) => {
    e.preventDefault();
    const id = e.target.getAttribute("data-delete-id");
    if (!id) return; // user clicked on something else
    this.removeTask(id);
  };

  addTask = (task) => {
    // state logic
    const id = String(Date.now());
    this.state.tasks = this.state.tasks.concat({ task, id });
    // UI logic
    this.updateHelp();
    const ul = document.querySelector("#taskul");
    const li = document.createElement("li");
    const span = document.createElement("span");
    const del = document.createElement("a");
    span.innerText = task;
    del.innerText = "delete";
    del.setAttribute("data-delete-id", id);
    ul.appendChild(li);
    li.appendChild(del);
    li.appendChild(span);
    this.items[id] = li;
  };

  removeTask = (id) => {
    const ul = document.querySelector("#taskul");
    // state logic
    this.state.tasks = this.state.tasks.filter((item) => item.id !== id);
    // UI logic
    this.updateHelp();
    const li = this.items[id];
    ul.removeChild(li);
  };

  // utility method
  updateHelp = () => {
    if (this.state.tasks.length > 0) {
      document.querySelector(".help").classList.add("hidden");
    } else {
      document.querySelector(".help").classList.remove("hidden");
    }
  };

  render() {
    return (
      <div id="taskList">
        <form id="taskForm" onSubmit={this.handleSubmit}>
          <input
            id="taskInput"
            type="text"
            value={this.state.value}
            onChange={this.handleChange}
          />
          <p className="help" id="taskHelp">
            Type in a task and then hit 'Enter'.
          </p>
          <ul id="taskul" onClick={this.handleDelete}></ul>
        </form>
      </div>
    );
  }
}

export default Tasks;