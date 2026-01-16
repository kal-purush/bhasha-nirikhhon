// [
//   Projects: {
//     id,
//     name,
//     description
//     todos :{
//       taks
//       deadline
//     }
//   }
// ]

export const addProject = (name, description) => {
  return {
    type: 'ADD_PROJECT' ,
    name: name,
    description: description
  };
};