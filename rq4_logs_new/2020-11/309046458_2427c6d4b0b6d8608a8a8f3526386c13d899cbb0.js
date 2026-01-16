//! qa

const initialTasks = [
  {
    _id: "5d2ca9e2e03d40b326596aa7",
    completed: true,
    body:
      "Occaecat non ea quis occaecat ad culpa amet deserunt incididunt elit fugiat pariatur. Exercitation commodo culpa in veniam proident laboris in. Excepteur cupidatat eiusmod dolor consectetur exercitation nulla aliqua veniam fugiat irure mollit. Eu dolor dolor excepteur pariatur aute do do ut pariatur consequat reprehenderit deserunt.\r\n",
    title: "Eu ea incididunt sunt consectetur fugiat non.",
  },
  {
    _id: "5d2ca9e29c8a94095c1288e0",
    completed: false,
    body:
      "Aliquip cupidatat ex adipisicing veniam do tempor. Lorem nulla adipisicing et esse cupidatat qui deserunt in fugiat duis est qui. Est adipisicing ipsum qui cupidatat exercitation. Cupidatat aliqua deserunt id deserunt excepteur nostrud culpa eu voluptate excepteur. Cillum officia proident anim aliquip. Dolore veniam qui reprehenderit voluptate non id anim.\r\n",
    title:
      "Deserunt laborum id consectetur pariatur veniam occaecat occaecat tempor voluptate pariatur nulla reprehenderit ipsum.",
  },
  {
    _id: "5d2ca9e2e03d40b3232496aa7",
    completed: true,
    body:
      "Occaecat non ea quis occaecat ad culpa amet deserunt incididunt elit fugiat pariatur. Exercitation commodo culpa in veniam proident laboris in. Excepteur cupidatat eiusmod dolor consectetur exercitation nulla aliqua veniam fugiat irure mollit. Eu dolor dolor excepteur pariatur aute do do ut pariatur consequat reprehenderit deserunt.\r\n",
    title: "Eu ea incididunt sunt consectetur fugiat non.",
  },
  {
    _id: "5d2ca9e29c8a94095564788e0",
    completed: false,
    body:
      "Aliquip cupidatat ex adipisicing veniam do tempor. Lorem nulla adipisicing et esse cupidatat qui deserunt in fugiat duis est qui. Est adipisicing ipsum qui cupidatat exercitation. Cupidatat aliqua deserunt id deserunt excepteur nostrud culpa eu voluptate excepteur. Cillum officia proident anim aliquip. Dolore veniam qui reprehenderit voluptate non id anim.\r\n",
    title:
      "Deserunt laborum id consectetur pariatur veniam occaecat occaecat tempor voluptate pariatur nulla reprehenderit ipsum.",
  },
];
const themes = {
  default: {
    "--body-bg-color": "#80bdff",
    "--base-text-color": "#212529",
    "--header-bg": "#007bff",
    "--header-text-color": "#fff",
    "--default-btn-bg": "#007bff",
    "--default-btn-text-color": "#fff",
    "--default-btn-hover-bg": "#0069d9",
    "--default-btn-border-color": "#0069d9",
    "--danger-btn-bg": "#dc3545",
    "--danger-btn-text-color": "#fff",
    "--danger-btn-hover-bg": "#bd2130",
    "--danger-btn-border-color": "#dc3545",
    "--input-border-color": "#ced4da",
    "--input-bg-color": "#fff",
    "--input-text-color": "#495057",
    "--input-focus-bg-color": "#fff",
    "--input-focus-text-color": "#495057",
    "--input-focus-border-color": "#80bdff",
    "--input-focus-box-shadow": "0 0 0 0.2rem rgba(0, 123, 255, 0.25)",
  },
  dark: {
    "--body-bg-color": "#141a20",
    "--base-text-color": "#212529",
    "--header-bg": "#343a40",
    "--header-text-color": "#fff",
    "--default-btn-bg": "#58616b",
    "--default-btn-text-color": "#fff",
    "--default-btn-hover-bg": "#292d31",
    "--default-btn-border-color": "#343a40",
    "--default-btn-focus-box-shadow": "0 0 0 0.2rem rgba(141, 143, 146, 0.25)",
    "--danger-btn-bg": "#b52d3a",
    "--danger-btn-text-color": "#fff",
    "--danger-btn-hover-bg": "#88222c",
    "--danger-btn-border-color": "#88222c",
    "--input-border-color": "#ced4da",
    "--input-bg-color": "#fff",
    "--input-text-color": "#495057",
    "--input-focus-bg-color": "#fff",
    "--input-focus-text-color": "#495057",
    "--input-focus-border-color": "#78818a",
    "--input-focus-box-shadow": "0 0 0 0.2rem rgba(141, 143, 146, 0.25)",
  },
  light: {
    "--body-bg-color": "#ced4da",
    "--base-text-color": "#212529",
    "--header-bg": "#fff",
    "--header-text-color": "#212529",
    "--default-btn-bg": "#fff",
    "--default-btn-text-color": "#212529",
    "--default-btn-hover-bg": "#e8e7e7",
    "--default-btn-border-color": "#343a40",
    "--default-btn-focus-box-shadow": "0 0 0 0.2rem rgba(141, 143, 146, 0.25)",
    "--danger-btn-bg": "#f1b5bb",
    "--danger-btn-text-color": "#212529",
    "--danger-btn-hover-bg": "#ef808a",
    "--danger-btn-border-color": "#e2818a",
    "--input-border-color": "#ced4da",
    "--input-bg-color": "#fff",
    "--input-text-color": "#495057",
    "--input-focus-bg-color": "#fff",
    "--input-focus-text-color": "#495057",
    "--input-focus-border-color": "#78818a",
    "--input-focus-box-shadow": "0 0 0 0.2rem rgba(141, 143, 146, 0.25)",
  },
};

(function (initialTasks) {
  /// UI Elements
  const form = document.forms["addTask"];
  const tbxTitle = form.elements["title"];
  const tbxBody = form.elements["body"];
  const slcTheme = document.getElementById("themeSelect");
  const container = document.querySelector(".container .row");
  const list = document.querySelector(".tasks-list-section .list-group");
  const ckbIncompleteTasks = document.querySelector(".checkboxLbl");

  /// Events
  form.addEventListener("submit", onSubmitFormHandler);
  function onSubmitFormHandler(e) {
    e.preventDefault();
    const title = tbxTitle.value;
    const body = tbxBody.value;

    if (!title || !body) {
      alert("Enter valid title and body");
      return;
    }

    let createdTask = setNewTask(title, body);
    checkEmptyLabelNeeded();
    checkTickNeeded();

    list.insertAdjacentElement("afterbegin", buildTask(createdTask));
    form.reset();
  }

  list.addEventListener("click", onDeleteBtnHandler);
  function onDeleteBtnHandler(e) {
    if (e.target.classList.contains("delete-btn")) {
      let taskWindow = e.target.closest("[data-task-id]");
      deleteElementFromHtml(taskWindow);
      deleteTaskFromStorage(taskWindow.dataset.taskId);
    }
  }

  list.addEventListener("click", onDoneBtnHandler);
  function onDoneBtnHandler({ target }) {
    if (target.classList.contains("done-btn")) {
      let taskWindow = target.closest("[data-task-id]");
      tasks[taskWindow.dataset.taskId].completed = true;

      taskWindow.style.setProperty("--list-item-bg", "mediumseagreen");
    }
  }

  slcTheme.addEventListener("change", onChangeSelectThemeHandler);
  function onChangeSelectThemeHandler() {
    setTheme(slcTheme.value);
  }

  ckbIncompleteTasks.addEventListener("click", onTickCkbIncompleteTasks);
  function onTickCkbIncompleteTasks({ target }) {
    if (target.checked) {
      document.querySelectorAll("li").forEach((task) => {
        if (tasks[task.dataset.taskId].completed) task.remove();
      });
    } else {
      let fragment = document.createDocumentFragment();

      Object.values(tasks)
        .filter((task) => task.completed)
        .forEach((task) => fragment.appendChild(buildTask(task)));
      list.appendChild(fragment);
    }
  }

  /// UI Methods
  function initialize() {
    if (!Object.keys(tasks).length) {
      checkEmptyLabelNeeded();
      return;
    }

    let fragment = document.createDocumentFragment();
    Object.values(tasks).forEach((task) => {
      fragment.appendChild(buildTask(task));
    });

    list.appendChild(fragment);
    checkTickNeeded();
  }

  function deleteElementFromHtml(elt) {
    elt.remove();
  }

  function setTheme(theme) {
    console.log(Object.entries(themes[theme]));
    Object.entries(themes[theme]).forEach(([key, value]) => {
      document.documentElement.style.setProperty(key, value);
    });
  }

  function checkEmptyLabelNeeded() {
    let lblEmpty = document.getElementById("empty-list");
    let tasksCount = Object.keys(tasks).length;

    if (!tasksCount && !lblEmpty)
      container.insertAdjacentElement("afterend", buildEmptyLabel());
    else if (tasksCount && lblEmpty) deleteElementFromHtml(lblEmpty);
  }

  function checkTickNeeded() {
    ckbIncompleteTasks.style.visibility = Object.keys(tasks).length
      ? "visible"
      : "hidden";
  }

  /// Inner Methods
  function deleteTaskFromStorage(id) {
    delete tasks[id];
    checkEmptyLabelNeeded();
    checkTickNeeded();
  }

  function buildTask({ _id, body, title, completed } = {}) {
    let li = document.createElement("li");
    li.classList.add(
      "list-group-item",
      "d-flex",
      "align-items-center",
      "flex-wrap",
      "mt-2"
    );
    li.setAttribute("data-task-id", _id);

    let span = document.createElement("span");
    span.textContent = title;
    span.style.fontWeight = "bold";

    let p = document.createElement("p");
    p.classList.add("mt-2", "w-100");
    p.textContent = body;

    let deleteBtn = document.createElement("button");
    deleteBtn.classList.add(
      "btn",
      "btn-danger",
      "ml-auto",
      "p-2",
      "delete-btn"
    );
    deleteBtn.textContent = "Delete";

    let doneBtn = document.createElement("button");
    doneBtn.classList.add("btn", "btn-success", "p-2", "done-btn");
    doneBtn.textContent = "Done";

    li.appendChild(span);
    li.appendChild(p);
    li.appendChild(doneBtn);
    li.appendChild(deleteBtn);

    if (completed) li.style.setProperty("--list-item-bg", "mediumseagreen");
    return li;
  }

  function buildEmptyLabel() {
    let a = document.createElement("a");
    a.classList.add("list-group-item", "mt-3");
    a.textContent = "No any todo items are found, please add some.";
    a.setAttribute("id", "empty-list");

    return a;
  }

  function setNewTask(title, body) {
    const newTask = {
      title,
      body,
      completed: false,
      _id: `task-${Math.random()}`,
    };

    tasks[newTask._id] = newTask;
    return { ...newTask };
  }

  let tasks = parseArrToObj(initialTasks);
  initialize(tasks);
})(initialTasks);

/// Global Utilities
function parseArrToObj(arr) {
  return arr.reduce((acc, item) => {
    acc[item._id] = item;
    return acc;
  }, {});
}