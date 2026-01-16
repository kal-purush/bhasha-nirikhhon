#!/usr/bin/env node

// Please note that this requires node to be installed in usr/bin/env

// Require modules
fs = require('fs');
path = require('path');

// Set up primary todo object for storage 
var currentTodos;

// Get arguments that are passed in
var args = process.argv.slice(2);

// Path to Desktop
var pathToDesktop = '/Users/alexyuningliu/Desktop'
var pathToList = path.join(pathToDesktop, 'todo.json') 

// Check if todo.json exists - if it doesn't, create a new file called todo.json in the Desktop

if (!fs.existsSync(pathToList)) {
  fs.writeFile(pathToList, JSON.stringify({todos: [{name: "walk dog"}]}));
  console.log('Created a todo.json on your Desktop!');   
}

// Read todo.json and store it in todo object
fs.readFile(pathToList, 'utf8', function (err, data) {
  if (err) {
    console.log('Error found: ', err);
  }
  currentTodos = JSON.parse(data);

  // Loop through the args and apply appropriate rules 

  switch(args[0]) {
    case 'ls':
      console.log('Here is your todo list: ', currentTodos)
      break;
    case 'commit':
      console.log('Starting a commit')
      currentTodos.todos.push({todos: args[1]})
      console.log('Updated Todo List: ', currentTodos)
      fs.writeFile(pathToList, JSON.stringify(currentTodos), function(err) {
        if (err) {
          return console.log('Write fail: ', err);
        }
        console.log('Write successful!');
      })
      break;
  }
})