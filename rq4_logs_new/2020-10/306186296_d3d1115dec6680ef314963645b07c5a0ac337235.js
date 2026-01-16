// CONTRUCTORS
const Employee = require('./lib/employee');
const Manager = require('./lib/manager');
const Engineer = require('./lib/engineer');
const Intern = require('./lib/intern');
// DEPENDENCIES
const axios = require('axios');
const Inquirer = require("inquirer");
const Jest = require('jest');
const path = require('path');
const fs = require('fs');

let managerArr = [];
let engineerArr = [];
let internArr = [];
let employeeInfo = [];



// QUESTIONS
const adminChoices = [
    {
        type: "list",
        message: "Would you like to:",
        name: "adminchoice",
        choices: [
            'Add an employee to the team?',
            'Create the team HTML page?'
        ]
    }
]

const adminQuestions = [
    {
        type: "input",
        message: "Hello manager, what is your name?",
        name: "name"
    },
    {
        type: "input",
        message: "What is your id?",
        name: "id"
    },
    {
        type: "input",
        message: "What is your email?",
        name: "email"
    },
    {
        type: "confirm",
        message: "Are you a manager?",
        name: "position",
        choices: [
            'Yes',
            'No'
        ]
    }
];

const questions = [
    {
        type: "input",
        message: "What is the employee's name?",
        name: "name"
    },
    {
        type: "input",
        message: "What is the employee's id?",
        name: "id"
    },
    {
        type: "input",
        message: "What is the employee's email?",
        name: "email"
    },
    {
        type: "list",
        message: "What is the employee's title?",
        name: "title",
        choices: [
            'engineer',
            'intern'
        ]
    }
];

const managerQuestion = [
    {
        type: "input",
        message: "What is your office number?",
        name: "officeNumber"
    }

];

const engineerQuestion = [
    {
        type: "input",
        message: "What is the employee's GitHUb username?",
        name: "gitname"
    }

];

const internQuestion = [
    {
        type: "input",
        message: "What school did the employee go to?",
        name: "school"
    }

];

let start =
    async function adminStart() {

        await Inquirer
            .prompt(adminQuestions)

            .then(async function (userData) {
                let managerInfo = {
                    'name': userData.name,
                    'id': JSON.parse(userData.id),
                    'email': userData.email,
                    'role': 'employee', // DEFAULT
                    'title': 'manager',
                    'officeNumber': '',
                    'gitname': '',
                    'github': '',
                    'school': ''

                }
                if (position = true) {
                    employeeInfo.push(managerInfo)
                    newemp()
                }
            })
    }

let next =
    async function adminNext() {
        await Inquirer
            .prompt(adminChoices)
            .then(async function (answers) {
                if (answers.adminchoice === 'Add an employee to the team?') {
                    employeeInfo.length = 0;
                    input()
                }
                if (answers.adminchoice === 'Create the team HTML page?') {
                    createteam()
                }
            })
    };

let input =
    async function init() {
        await Inquirer
            .prompt(questions)

            .then(async function (userData) {
                let userInfo = {
                    'name': userData.name,
                    'id': JSON.parse(userData.id),
                    'email': userData.email,
                    'role': 'employee', // DEFAULT
                    'title': userData.title,
                    'officeNumber': '',
                    'gitname': '',
                    'github': '',
                    'school': ''
                }
                employeeInfo.push(userInfo)
                newemp()
            })
    };

let newemp =
    async function employeeprofile() {
        const name = employeeInfo[0].name;
        const id = employeeInfo[0].id;
        const email = employeeInfo[0].email;
        const role = employeeInfo[0].role;

        const employee = new Employee(name, id, email, role)
        classdir()
    };

let classdir =
    async function bytitle() {

        if (employeeInfo[0].title === "manager") {
            buildManager()
        }
        if (employeeInfo[0].title === "engineer") {
            buildEngineer()
        }
        if (employeeInfo[0].title === "intern") {
            buildIntern()
        }
    };

async function buildManager() {

    await Inquirer
        .prompt(managerQuestion)

        .then(async function (userData) {
            let managerAns = {
                'officeNumber': JSON.parse(userData.officeNumber)
            }
            employeeInfo[0].officeNumber = managerAns.officeNumber;

            const name = employeeInfo[0].name;
            const id = employeeInfo[0].id;
            const email = employeeInfo[0].email;
            const role = employeeInfo[0].role;
            const officeNumber = employeeInfo[0].officeNumber;
        
            const manager = new Manager(name, id, email, officeNumber)
            managerArr.push(manager);

        })

    next()
};

async function buildEngineer() {
    await Inquirer
        .prompt(engineerQuestion)

        .then(async function (userData) {
            let engineerInfo = {
                'gitname': userData.gitname
            }
            employeeInfo[0].gitname = engineerInfo.gitname;
        })
        .then(async function() {

            const gitname = employeeInfo[0].gitname;
            let queryURL = 'https://api.github.com/users/' + gitname;
            axios
                .get(queryURL).then(async function (response) {
                    const engineerInfo = {
                        "github": response.data.login,
                    }
        
                    employeeInfo[0].github = engineerInfo.github;
                    
                })
        })
                        setTimeout(function(){
                        const name = employeeInfo[0].name;
                        const id = employeeInfo[0].id;
                        const email = employeeInfo[0].email;
                        const role = employeeInfo[0].role;
                        const gitname = employeeInfo[0].gitname;
                        const github = employeeInfo[0].github;
                    
                        const engineer = new Engineer(name, id, email, gitname, github)
             
                        engineerArr.push(engineer)
                        }, 2000);
                    
next()
};

async function buildIntern() {
    await Inquirer
        .prompt(internQuestion)

        .then(async function (userData) {
            let internInfo = {
                'school': userData.school
            }
            employeeInfo[0].school = internInfo.school;
        })
        const name = employeeInfo[0].name;
        const id = employeeInfo[0].id;
        const email = employeeInfo[0].email;
        const role = employeeInfo[0].role;
        const school = employeeInfo[0].school;

    const intern = new Intern(name, id, email, school);
    internArr.push(intern)
    next()
};

createteam =
    async function teamHTML() {


        fs.writeFileSync('./output/teampage.html',
            '<DOCTYPE! HTML>' +
            '<html>' +
            '<head>' +
            '<meta charset="UTF-8">' +
            '<link rel="stylesheet" type="text/css" href="style.css">' +
            '<link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous"/>' +
            '<meta name="viewport" content="width=device-width, initial-scale=1.0"/> ' +
            '<meta http-equiv="X-UA-Compatible" content="ie=edge" />' +
            '</head>' +
            '<body>' +
            '<header>' +
            '<h1>' + 'Company Team Page' + '</h1>' +
            '</header>' +
            '<container>' +
            '<div class="row">' +
            '<div class="col-sm-10">'
        );

        fs.appendFileSync('./output/teampage.html',
            '<div id="manager">' +
            '<div class="card">' +
            '<div class="card-header bg-info">' + managerArr[0].name + '</div>' +
                '<div class="card-body">' +
                    '<div class=content>' +

                    '<p>' + "ID: " + managerArr[0].id + '</p>' + '<hr>' +
                    '<p>' + "Email: " + managerArr[0].email + '</p>' + '<hr>' +
                    '<p>' + "Office Number: " + managerArr[0].officeNumber + '</p>' + '<hr>' +

                    '</div>' +
                '</div>' +
            '<div class="card-footer bg-info">' + "Manager" + '</div>' +
            '</div>' +
            '</div>'
        );

        for (i = 0; i < engineerArr.length; i++) {
            fs.appendFileSync('./output/teampage.html',
                '<div id="engineer">' +
                '<div class="card">' +
                '<div class="card-header bg-primary">' + engineerArr[i].name + '</div>' +
                '<div class="card-body">' +
                '<div class=content>' +

                '<p>' + "ID: " + engineerArr[i].id + '</p>' + '<hr>' +
                '<p>' + "Email " + engineerArr[i].email + '</p>' + '<hr>' +
                '<p>' + "GitHub username: " + engineerArr[i].gitname + '</p>' + '<hr>' +

                '</div>' +
                '</div>' +
                '<div class="card-footer bg-primary">' + 'Engineer' + '</div>' +
                '</div>' +
                '</div>'
            );
        }

        for (i = 0; i < internArr.length; i++) {
            fs.appendFileSync('./output/teampage.html',
                '<div id="intern">' +
                '<div class="card">' +
                '<div class="card-header bg-success">' + internArr[i].name + '</div>' +
                '<div class="card-body">' +
                '<div class=content>' +

                '<p>' + "ID: " + internArr[i].id + '</p>' + '<hr>' +
                '<p>' + "Email: " + internArr[i].email + '</p>' + '<hr>' +
                '<p>' + "School: " + internArr[i].school + '</p>' + '<hr>' +

                '</div>' +
                '</div>' +
                '<div class="card-footer bg-success">' + 'Intern' + '</div>' +
                '</div>' +
                '</div>'
            );
        }

        fs.appendFileSync('./output/teampage.html', 
            '</div>' +
            '</div>' +
            '</container>' +
            '</body>' +
            '</html>'
        );

        console.log('Your html file for the team page is in the output folder')
    }

start()