const mysql = require("inquirer");
const mysql = require("mysql");

const connection = mysql.createConnection({
  host: "localhost",
  port: 3306,
  user: "root",
  password: "root",
  database: "employeemanagementDB",
});

connection.connect((err) => {
  if (err) throw err;
  askQuestions();
});

// present to the user in the terminal
const askQuestions = () => {
  inquirer
    .prompt({
      name: "list",
      type: "task",
      message: "What would you like to do?",
      choices: [
        "View all employee",
        "View employees by department",
        "Add employee",
        "Remove Employee",
        "Update employee role",
        "Add role",
        "End"]
    })

    .then((answer) => {
        //we need to build functionality based on user choice
        switch (answer.action) {
          case "View all employee":
            viewEmployee();
            break;

          case "View employee by department":
            employeeByDepartment();
            break;

          case "Add employee":
            addEmployee();
            break;

          case "Remove Employee":
            removeEmployee();
            break;

          case "Update employee role":
            updateEmployeeRole();
            break;

          case "Add role":
            addRole();
            break;

            default:
            connection.end()
            break;
        }
      });
  };

const viewEmployee = () => {
    return `SELECT e.id, e.first-name, e.last_name, r.title AS department, r.salary, CONCAT(m.first_name, ' ', m.last_name) AS Manager
    FROM employee e
        LEFT JOIN employee m ON m.id = e.manager_id
        LEFT JOIN Role r ON e.role_id = r.id
        LEFT JOIN Department d ON d.id = r.department_id
    ORDER BY e.id ASC;`;
};


    
    
    
    
    // connection.query(query, ("SELECT * FROM employee",  )
   
    //   .then((answer) => {
    //     //use convenience variable
    //     // const query = "SELECT position, song, year FROM top5000 WHERE ?";
    //     connection.query(query, { artist: answer.artist }, (err, res) => {
    //       res.forEach(({ position, song, year }) => {
    //         console.log(
          


// function viewEmployee() {
//     console.log("View all employee\n");

// }