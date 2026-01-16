const todosList = document.querySelector("#todo-list");
const newTodoForm = document.querySelector("#todo-item-form");
const renderCountEl = document.querySelector("#render-count");
const nextIdEl = document.querySelector("#next-id");

// A counter to show how often the page has been updated since it was loaded
// This increases every time a CRUD operation occurs
let renderCount = 1;

// We have a predefined set of todos!
let todos = [
  { id: 1, title: "Feed the cat", completed: true },
  { id: 2, title: "Cut the grass", completed: false },
  { id: 3, title: "Fix the leaky roof!", completed: false },
];

let nextId = Math.max(...todos.map((obj) => obj.id)) + 1;

// Set up form listener to create a new TODO

newTodoForm.addEventListener("submit", (ev) => {
  ev.preventDefault();

  console.log(ev.target);
  // console.log(ev.target.reset)

  // NOTE: These values depend on the HTML
  const TITLE = 0;
  const DONE = 1;

  const todoTitle = ev.target[TITLE].value;
  const todoDone = ev.target[DONE].checked;

  // Clear the form now that we have the TODO title
  ev.target.reset();

  // Don't add empty TODOs
  if (!todoTitle) return;

  todos.push({ id: nextId++, title: todoTitle, completed: todoDone });

  render();
});

// Helper functions ...

function toggleCompleted(todo) {
  todo.completed = !todo.completed;

  render(); // Re-display the TODO list
}

function deleteTodo(todo) {
  todos = todos.filter((it) => it.id !== todo.id);

  render(); // Re-display the TODO list
}

// TODO List rendering

function clearTodosList() {
  todosList.innerHTML = "";
}

function createTodoListElement(todo) {
  // We are going to create the following ...
  // <li><span>{TODO title}</span><button>Del</button></li>
  //
  // Clicking the TODO will toggle its 'completed' status
  // 'Del' button will delete it

  const li = document.createElement("li");

  const span = document.createElement("span");

  span.innerText = `${todo.title} (${todo.id})`;
  if (todo.completed) {
    span.className = "completed";
  } else {
    span.className = "todo";
  }

  span.addEventListener("click", (ev) => {
    toggleCompleted(todo);
  });

  li.appendChild(span);

  const deleteButton = document.createElement("button");
  deleteButton.innerText = "Del";
  deleteButton.className = "deleteTodo";

  deleteButton.addEventListener("click", (ev) => {
    ev.preventDefault();
    if (window.confirm(`Do you want to delete this TODO? - ${todo.title}`))
      deleteTodo(todo);
  });

  li.appendChild(deleteButton);

  return li;
}

function setStats() {
  renderCountEl.innerText = renderCount;
  nextIdEl.innerText = nextId;
}

function render() {
  clearTodosList();

  todos.forEach((todo) => {
    todosList.appendChild(createTodoListElement(todo));
  });

  setStats();
  renderCount++;
}

// Initialise ...

render();

// DONE: Load initial items and populate list
// DONE: Set up submit handler to creat a new todo
//         Send to server and display (re-render all?)
//         Clear form for the next one

// DONE: Add ability to mark todo as complete
// DONE: Add ability to delete a todo