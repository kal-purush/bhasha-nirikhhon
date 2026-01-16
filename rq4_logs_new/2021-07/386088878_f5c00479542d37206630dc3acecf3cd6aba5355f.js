const inquirer = require('inquirer');
const fs = require('fs');
const Manager = require('./lib/Manager.js');
const Engineer = require('./lib/Engineer.js');
const Intern = require('./lib/Intern.js');
let team = []; //team members stored as objects in team array

//generates html to be saved
const generateHTML = () => {
  //html before member cards
  let htmlText = `
    <!DOCTYPE html>

    <html>
      <head>
        <meta charset="utf-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <title>My Team</title>
        <meta name="description" content="">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="stylesheet" href="./css/reset.css">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/css/materialize.min.css">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
        <link rel="stylesheet" href="./css/style.css">
      </head>
      <body>
        <header class="red white-text">My Team</header>
        <div class="card-area">`;

  //html for member cards
  team.forEach(member => {
    let role = member.getRole();
    let detail = [];//[name of job's unique detail, function to call detail, font awesome icon name]

    //picks role of employee
    if(role === 'Manager'){detail = ['Office Number', member.getOfficeNumber(), 'coffee'];}
    else if(role === 'Engineer'){detail = ['Github', `<a href="https://github.com/${member.getGithub()}" target="_blank">${member.getGithub()}</a>`, 'cog'];}
    else {detail = ['School', member.getSchool(), 'graduation-cap'];}

    htmlText+=`
      <div class="card">
        <div class="card-head">
          <div class="name">${member.getName()}</div>
          <div class="position">
            <i class="fa fa-${detail[2]}"></i>
            <span>${role}</span>
          </div>
        </div>
        <div class="card-body">
          <div class="info">
            <div class="id">ID: ${member.getId()}</div>
            <div class="email">Email: <a href="mailto:${member.getEmail()}">${member.getEmail()}</a> </div>
            <div class="detail">${detail[0]}: ${detail[1]}</div>
          </div>
        </div>
      </div>`;
  });

  //html after member cards
  htmlText += `
      </div>
      <script src="https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/js/materialize.min.js"></script>
      <script src="./index.js"></script>
    </body>
    </html>`;

  //writes file
  fs.writeFile('team.html', htmlText, (err) =>
    err ? console.log(err) : console.log('Generating HTML...')
  );
};

//function to add member
let addMember = (role) =>{
  //picks unique detail based on role
  let detail = "school";
  if(role == "manager"){detail = "office number";}
  else if(role == "engineer"){detail = "github";}

  //asks questions to record new member
  inquirer
    .prompt([
      {
        type: 'input',
        name: 'name',
        message: `What is the ${role}'s name?`,
      },
      {
        type: 'input',
        name: 'id',
        message: 'What is the employee id?',
      },
      {
        type: 'input',
        name: 'email',
        message: `What is the ${role}'s email address?`,
      },
      {
        type: 'input',
        name: 'detail',
        message: `What is the ${role}'s ${detail}?`,
      },
    ])
    .then((res) => {
      let m = res;
      
      //adds new member to team array
      if(role === 'manager'){team.push(new Manager(m.name, m.id, m.email, m.detail));}
      else if(role === 'engineer'){team.push(new Engineer(m.name, m.id, m.email, m.detail));}
      else {team.push(new Intern(m.name, m.id, m.email, m.detail));}

      //asks if a new member should be added
      inquirer.prompt([{
        type: 'list',
        name: 'nextMember',
        message: 'Add an engineer, an intern, or finish building team',
        choices: ['engineer', 'intern', 'finished'],
      }])
      .then((res) => {
        //function recalls itself recursively to add new member
        //otherwise generates html
        if(res.nextMember != 'finished'){addMember(res.nextMember);}
        else {generateHTML();}
      });
    });
};

//code triggered br initial call to addMember function
addMember('manager');