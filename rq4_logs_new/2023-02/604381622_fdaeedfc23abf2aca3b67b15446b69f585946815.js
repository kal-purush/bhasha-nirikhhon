// import all tools need
const Manager = require('./lib/manager.js');
const Engineer = require('./lib/engineer.js');
const Intern = require('./lib/intern.js');
const inquirer = require('inquirer');
const path = require('path');
const fs = require('fs');
const outputDir = path.resolve(__dirname, 'dist');
outputPath = path.join(outputDir, 'index.html');
const teamGenerator = require('./src/generateTeamTemp.js');


teamArrayData = [];

function App() {

  function makeTeam() {
    inquirer.prompt([ {
      type: "list",
      message: "What type of employee would you like to add to your team?",
      name: "addEmployeePrompt",
      choices: [ "Manager", "Engineer", "Intern", "No more team members are needed." ]
    } ]).then(function (userInput) {
      switch (userInput.addEmployeePrompt) {
        case "Manager":
          addManager();
          break;
        case "Ingineer":
          addEngineer();
          break;
        case "Intern":
          addIntern();
          break;

        default:
          htmlBuilder();
      }
    })
  }
  // Manager prompt

  function addManager() {
    inquirer.prompt([

      {
        type: "input",
        name: "managerName",
        message: "What is the manager's name?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      },

      {
        type: "input",
        name: "managerId",
        message: "What is the manager's employee ID number?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      },

      {
        type: "input",
        name: "managerEmail",
        message: "What is the manager's email address?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      },

      {
        type: "input",
        name: "managerOfficeNumber",
        message: "What is the manager's office number?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      }

    ]).then(answers => {
      const manager = new Manager(answers.managerName, answers.managerId, answers.managerEmail, answers.managerOfficeNumber);
      teamArrayData.push(manager);
      makeTeam();
    });

  }

  // Engineer prompt
  function addEngineer() {
    inquirer.prompt([

      {
        type: "input",
        name: "engineerName",
        message: "What is the engineer's name?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      },

      {
        type: "input",
        name: "engineerId",
        message: "What is the engineer's employee ID number?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      },

      {
        type: "input",
        name: "engineerEmail",
        message: "What is the engineer's email address?", 
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      },

      {
        type: "input",
        name: "engineerGithub",
        message: "What is the engineer's GitHub username?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      }

    ]).then(answers => {
      const engineer = new Engineer(answers.engineerName, answers.engineerId, answers.engineerEmail, answers.engineerGithub);
      teamArrayData.push(engineer);
      makeTeam();
    });

  };
  // intern prompt
  function addIntern() {
    inquirer.prompt([

      {
        type: "input",
        name: "internName",
        message: "What is the intern's name?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      },

      {
        type: "input",
        name: "internId",
        message: "What is the intern's employee ID number?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      },

      {
        type: "input",
        name: "internEmail",
        message: "What is the intern's email address?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      },

      {
        type: "input",
        name: "internSchool",
        message: "What school does the intern attend?",
        validate: input => {
          if (input) {
            return true;
          } else {
            alert('Please Enter Input Value')
          }
        }
      }

    ]).then(answers => {
      const intern = new Intern(answers.internName, answers.internId, answers.internEmail, answers.internSchool);
      teamArrayData.push(intern);
      makeTeam();
    });

  };


  function htmlBuilder() {
    console.log("Team Your Team is Built");

    fs.writeFileSync(outputPath, teamGenerator(teamArrayData), "UTF-8")

  }

  makeTeam();

}

App();