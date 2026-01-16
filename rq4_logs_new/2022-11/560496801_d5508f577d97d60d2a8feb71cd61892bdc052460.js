class ComplatedTasks {
  static checkTask(tasksArr) {
    const tasksList = document.querySelector('.todo-list');
    const checkBox = document.querySelectorAll('input[name=checkbox]');

    if (tasksList.hasChildNodes()) {
      checkBox.forEach((check) => {
        check.addEventListener('change', (e) => {
          const checkMark = check.nextElementSibling;
          const taskName = check.nextElementSibling.nextElementSibling;
          const taskUiId = check.parentElement.parentElement.dataset.id;
          if (e.target.checked) {
            taskName.style.textDecoration = 'line-through';
            check.style.display = 'none';
            checkMark.style.display = 'block';
            // looping over the state arr
            tasksArr.forEach((task) => {
              if (task.index === Number(taskUiId)) {
                task.isCompleted = true;
                localStorage.setItem('tasks', JSON.stringify(tasksArr));
              }
            });
          }
        });
      });
    }
  }

  static taskDecoration(tasksArr) {
    const tasksList = document.querySelector('.todo-list');
    const checkBox = document.querySelectorAll('input[name=checkbox]');

    if (tasksList.hasChildNodes()) {
      checkBox.forEach((check) => {
        const checkMark = check.nextElementSibling;
        const taskName = check.nextElementSibling.nextElementSibling;
        const taskUiId = check.parentElement.parentElement.dataset.id;
        tasksArr.forEach((task) => {
          if (task.isCompleted && task.index === Number(taskUiId)) {
            taskName.style.textDecoration = 'line-through';
            check.style.display = 'none';
            checkMark.style.display = 'block';
          }
        });
      });
    }
  }
}

export default ComplatedTasks;