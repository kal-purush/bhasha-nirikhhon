const addRoleQuestions = (options)=> {
   return  [
        {
          name: "jobTitle",
          type: "input",
          message: "What is the name of the role?",
        },
        {
          name: "salary",
          type: "input",
          message: "What is the salary for this role?",
        },
        {
          name: "department",
          type: "list",
          message: "Which department does the role belong to?",
          choices: options,
        },
      ]
} 
addDepartmentQuestions = [
    {
      name: "department",
      type: "input",
      message: "What is the department name?",
      validate: (input) => {
        if (input) {
          return true;
        } else {
          console.log("Please enter the department name!");
          return false;
        }
      },
    },
  ]

module.exports= {addRoleQuestions, addDepartmentQuestions}