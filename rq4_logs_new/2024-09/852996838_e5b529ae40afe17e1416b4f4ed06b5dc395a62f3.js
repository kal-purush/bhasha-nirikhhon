import { Command } from "commander";
import fs from "fs";

const program = new Command();
const taskFile = "./tasks.json";

if (!fs.existsSync(taskFile)) {
  fs.writeFileSync(taskFile, "[]", "utf-8");
}

function addTodo(task) {
  const tasks = JSON.parse(fs.readFileSync(taskFile, "utf-8"));
  tasks.push({ task, done: false });
  fs.writeFileSync(taskFile, JSON.stringify(tasks, null, 2), "utf-8");
  console.log(`Task added: ${task}`);
}

function deleteTodo(task) {
  let tasks = JSON.parse(fs.readFileSync(taskFile, "utf-8"));
  tasks = tasks.filter((t) => t.task !== task);
  fs.writeFileSync(taskFile, JSON.stringify(tasks, null, 2), "utf-8");
  console.log(`Task deleted: ${task}`);
}

function updateTask(task, newTask) {
  let tasks = JSON.parse(fs.readFileSync(taskFile, "utf-8"));
  const taskIndex = tasks.findIndex((t) => t.task === task);
  if (taskIndex > -1) {
    tasks[taskIndex].task = newTask;
    fs.writeFileSync(taskFile, JSON.stringify(tasks, null, 2), "utf-8");
    console.log(`Task updated: ${task} to ${newTask}`);
  } else {
    console.log("Here is all of your tasks");
    console.log(`Task not found: ${task}`);
  }
}

function cleanList() {
  fs.writeFileSync(taskFile, "[]", "utf-8");
}

function showTasksList() {
  const tasks = JSON.parse(fs.readFileSync(taskFile, "utf-8"));
  if (tasks.length === 0) {
    console.log("No task has been added");
  } else {
    console.log("Here are your tasks");
    tasks.forEach((tasks, index) => {
      console.log(
        `${index + 1}. ${tasks.task} - ${tasks.done ? "done" : "pending"}`,
      );
    });
  }
}

function toggleTaskCompletion(task) {
  let tasks = JSON.parse(fs.readFileSync(taskFile, "utf-8"));
  const taskIndex = tasks.findIndex((t) => t.task === task);
  if (taskIndex > -1) {
    tasks[taskIndex].done = !tasks[taskIndex].done;
    fs.writeFileSync(taskFile, JSON.stringify(tasks, null, 2), "utf-8");
    console.log(`Task is now toggled: ${task}`);
  } else {
    console.log("There were no task found");
  }
}
program
  .version("0.1.0")
  .description(
    "A command-line task manager that allows you to manage your to-do list efficiently(not efficiently lol). With this tool, you can add new tasks, delete existing ones, update task descriptions, toggle task completion status, clean all tasks, and view the list of all tasks (you damn nerd, just use todo microsoft). Ideal for keeping track of tasks directly from the terminal. (definitely not ideal"
  );

program
  .command("t")
  .option("-a, --add <task>", "Add a task to the registry")
  .option("-d, --delete <task>", "Delete a task from the registry")
  .option("-u, --update <task> <newTask>", "Update a task in the registry")
  .option("-cl --cleanList", "cleans all the task")
  .option("-sl, --showList", "show all the list present in registory")
  .option("-t --toggle <task>", "toggle task status")
  .action((options) => {
    if (options.add) {
      addTodo(options.add);
    } else if (options.delete) {
      deleteTodo(options.delete);
    } else if (options.update) {
      const [task, newTask] = program.args.slice(-2); // Capture the last two arguments
      if (task && newTask) {
        updateTask(task, newTask);
      } else {
        console.log(
          "Please provide both the task to update and the new task name.",
        );
      }
    } else if (options.cleanList) {
      cleanList();
    } else if (options.showList) {
      showTasksList();
    } else if (options.toggle) {
      toggleTaskCompletion(options.toggle);
    }
  });

program.parse(process.argv);