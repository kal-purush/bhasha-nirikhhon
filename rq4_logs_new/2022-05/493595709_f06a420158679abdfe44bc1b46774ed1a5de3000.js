// fetch(url)
//   .then((response) => {
//     // handle the response
//   })
//   .catch((error) => {
//     // handle the error
//   });
// fetch("http://localhost:3000/student")
//   .then((response) => response.json())
//   .then((json) => console.log(json))
//   .catch((error) => {
//     // handle the error
//   });
fetch("http://localhost:3000/student")
  .then((response) => response.json())
  .then((data) => console.log(data));