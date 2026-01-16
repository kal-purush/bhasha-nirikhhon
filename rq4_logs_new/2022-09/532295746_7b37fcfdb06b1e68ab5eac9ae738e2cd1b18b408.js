const express = require('express');
const mysql = require('mysql2');
const inquirer = require("inquirer")
const port = process.env.PORT || 3001;
const app = express();

//requires
// ----------------------------------------------------------------------- //

app.use(express.urlencoded({ extended: false }));
app.use(express.json());

//middleware
// ----------------------------------------------------------------------- //


const db = mysql.createConnection(
  {
    host: 'localhost',
    user: 'root',
    password: 'bradley4147',
    database: 'team_db'
  },
  console.log(`Connected to the team_db.`)
);

//db
// ----------------------------------------------------------------------- //

const promptScreen = () => {
  return inquirer.prompt([
    {
      type: "list",
      message: "What Would You Like to Do?",
      choices: [
        "View All Employees",
        "View All Roles",
        "View All Departments",
        "Add Employee",
        "Add Role",
        "Add Department",
        "Update Employee Role",
        "Quit"
      ],
      name: "employeeAdd",
      
    }
  ]).then((newCase) => {
    switch (newCase.employeeAdd) {
      case "View All Employees":
        viewEmployees()
        break;
      case "Add Employee":
        addEmployee()
        break;
      case "Update Employee Role":
        updateEmployeeRole()
        break;
      case "View All Roles":
        viewAllRoles()
        break;
      case "Add Role":
        addRole()
        break;
      case "View All Departments":
        viewAllDepartments()
        break;
      case "Add Department":
        addDept()
        break;
      case "Quit":
        quit();
    }
  })
}

//prompt
// ----------------------------------------------------------------------- //


const viewEmployees = () => {
  const sql = `Select * from employees`;
  db.query(sql, function(err, res) {
    if (err) throw err;
    console.table(res);
    promptScreen();
  });
}

const viewAllRoles = () => {
  const sql = `select * from roles`;
  db.query(sql, function(err, res) {
    if (err) throw err;
    console.table(res);
    promptScreen();
  });
}

const viewAllDepartments = () => {
  const sql = `select * from department`;
  db.query(sql, function(err, res) {
    if (err) throw err;
    console.table(res);
    promptScreen();
  });
}


//view prompts
// ----------------------------------------------------------------------- //


const addEmployee = () => {
  return inquirer.prompt([
    {
      type: "input",
      message: "New Employee's first name",
      name: "newEmpFName"
    },
    {
      type: "input",
      message: "New Employee's last name",
      name: "newEmpLName"
    },
    {
      type: "input",
      message: "What is the employees role? (enter role id)",
      name: "newEmpRole"
    },
    {
      type: "input",
      message: "What manager will this new employee be working under? (enter managers id)",
      name: "newEmpMan"
    },
  ]).then(function (answer) {

    db.query("INSERT INTO employees (first_name, last_name, role_id, manager_id) VALUES (?, ?, ?, ?)", [answer.newEmpFName, answer.newEmpLName, answer.newEmpRole, answer.newEmpMan], function (err, res) {
      if (err)
      console.table(res);
      promptScreen()
    })
  })
}


const addDept = () => {
  return inquirer.prompt([
    {
      type: "input",
      message: "Name of New Department",
      name: "newDeptName"
    },
  ]).then(function (answer) {

    db.query("INSERT INTO department (name) VALUES (?)", [answer.newDeptName], function (err, res) {
      if (err)
      console.table(res);
      promptScreen()
    })
  })
}


const addRole = () => {
  return inquirer.prompt([
    {
      type: "input",
      message: "Name of New Role",
      name: "newRoleName"
    },
    {
      type: "input",
      message: "Input Salary of New Role",
      name: "newRoleSalary"
    },
    {
      type: "input",
      message: "What is the department ID number?",
      name: "departmentId",
    }
  ]).then(function (answer) {

    db.query("INSERT INTO roles (title, salary, department_id) VALUES (?, ?, ?)", [answer.newRoleName, answer.newRoleSalary, answer.departmentId], function (err, res) {
      if (err)
      console.table(res);
      promptScreen()
    })
  })
}

//add prompst
// ----------------------------------------------------------------------- //


const updateEmployeeRole = () => {
  inquirer
  .prompt([
    {
      type: "input",
      message: "Which employee would you like to update?",
      name: "nameUpdate"
    },

    {
      type: "input",
      message: "What is the employees new role id?",
      name: "updateRole"
    }
  ])
  .then(function(answer) {
    db.query('UPDATE employees SET role_id=? WHERE first_name= ?',[answer.updateRole, answer.nameUpdate],function(err, res) {
      if (err)
      console.table(res);
      promptScreen();
    });
  });
}

function quit() {
  db.end();
  process.exit();
}

//update and quit prompts
// ----------------------------------------------------------------------- //
//picking at my 6-string

app.use((req, res) => {
  res.status(404).end();
});

//  server  
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
})

promptScreen()