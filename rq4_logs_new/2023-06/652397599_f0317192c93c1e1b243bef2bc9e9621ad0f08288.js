const mysql = require('mysql2');
const inquirer = require('inquirer');
require('dotenv').config();

// Tried to get this to work but getting choices to populate was taking too long 
// const { menu, newEmployee, updateEmployee, newRole, newDepartment } = require('./lib/prompts.js');

const db = mysql.createConnection(
    {
        host: 'localhost',
        user:  process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: 'employee_db'
    }
);

const dataEntry = () => {
    inquirer.prompt([{
        type: 'list',
        name: 'option',
        message: 'What would you like to do?',
        choices: [
            '1. View All Employee',
            '2. Add Employee',
            '3. Update Employee Role',
            '4. View All Roles',
            '5. Add Role',
            '6. View All Departments',
            '7. Add Department',
            '0. Quit'
        ]
    }])
        .then((data) => {
            if (data.option === '1. View All Employee') {
                db.query('SELECT * FROM employees', (err, employees) => {
                    console.log(employees.map(a => `${a.first_name} ${a.last_name}`));
                    dataEntry();
                });
            }
            if (data.option === '2. Add Employee') {
                addEmployee();
            }
            if (data.option === '3. Update Employee Role') {
                updateEmployee();
            }
            if (data.option === '4. View All Roles') {
                db.query('SELECT * FROM roles', (err, roles) => {
                    console.log(roles.map(a => a.title));
                    dataEntry();
                });
            }
            if (data.option === '5. Add Role') {
                addRole();
            }
            if (data.option === '6. View All Departments') {
                db.query('SELECT * FROM departments', (err, departments) => {
                    console.log(departments.map(a => a.name));
                    dataEntry();
                });
            }
            if (data.option === '7. Add Department') {
                addDepartment();
            }
            if (data.option === '0. Quit') {
                console.log('Okay');
                process.exit(0);
            }
        });
}

const addEmployee = () => {
    db.query('SELECT id, title FROM roles', (err, roles) => {
        const roleChoice = roles.map(({ id, title }) => ({ name: title, value: id }))
        db.query('SELECT first_name, last_name, id from employees', (err, employee) => {
            const managers = employee.map(({ first_name, last_name, id }) => ({ name: `${first_name} ${last_name}`, value: id }));
            inquirer.prompt([
                {
                    type: 'input',
                    name: 'firstName',
                    message: 'First name?'
                },
                {
                    type: 'input',
                    name: 'lastName',
                    message: 'Last name?'
                },
                {
                    type: 'list',
                    name: 'role',
                    message: 'What is their role?',
                    choices: roleChoice
                },
                {
                    type: 'list',
                    name: 'manager',
                    message: 'Who is their manager?',
                    choices: managers
                }
            ])
                .then((results) => {
                    console.log(`Adding employee ${results.firstName} ${results.lastName}`);
                    db.query(`INSERT INTO employees (first_name, last_name, role_id, manager_id) VALUES ("${results.firstName}", "${results.lastName}", ${results.role}, ${results.manager});`, (err, results) => {
                        err ? console.log(err) : dataEntry();
                    });
                })
        })

    })
};

const updateEmployee = () => {
    db.query('SELECT first_name, last_name, id from employees', (err, data) => {
        const employees = data.map(({ first_name, last_name, id }) => ({ name: `${first_name} ${last_name}`, value: id }));
        inquirer.prompt([
            {
                type: 'list',
                name: 'employee',
                message: 'First name?',
                choices: employees
            },
        ])
            .then((result) => {
                db.query('SELECT id, title FROM roles', (err, roles) => {
                    const roleChoice = roles.map(({ id, title }) => ({ name: title, value: id }))
                    inquirer.prompt([
                        {
                            type: 'list',
                            name: 'role',
                            message: 'What role?',
                            choices: roleChoice
                        }
                    ])
                        .then((inputs) => {
                            console.log(`Updating role`)
                            db.query(`UPDATE employees SET role_id = ${inputs.role}
                        WHERE id = ${result.employee}`)
                            dataEntry();
                        })
                })
            })
    })
}

const addRole = () => {
    db.query('SELECT * from departments', (err, data) => {
        const departments = data.map(({ id, name }) => ({ name: `${name}`, value: id }));
        inquirer.prompt([
            {
                type: 'input',
                name: 'title',
                message: 'What is the new role?'
            },
            {
                type: 'input',
                name: 'salary',
                message: 'What is the salary?'
            },

            {
                type: 'list',
                name: 'department',
                message: 'Which department does this new role belong to?',
                choices: departments
            }
        ])
            .then((results) => {
                db.query(`INSERT INTO roles (title, salary, department_id) VALUES ("${results.title}", ${results.salary}, ${results.department})`);
                console.log(`Adding new role`)
                dataEntry();
            })
    })
}

const addDepartment = () => {
    inquirer.prompt(
        {
            type: 'input',
            name: 'department',
            message: 'What is the name of the department?'
        }
    )
        .then((data) => {
            db.query(`INSERT INTO departments (name) VALUES ('${data.department}')`);
            console.log(`Adding new department`)
            dataEntry();
        })
}

console.clear();
dataEntry();