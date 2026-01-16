// PROMISES
// Promise return edilir
// Return edilirken resolve reject kullanılır
// return edilen fonksiyon .then ile başarılı olursa .catch ile başarısız olursa

const getTodos = (resource) => {
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();

    request.addEventListener("readystatechange", () => {
      if (request.readyState === 4 && request.status === 200) {
        const data = JSON.parse(request.responseText);
        resolve([undefined, data]);
      } else if (request.readyState === 4) {
        reject(["Bir hata oluştu", undefined]);
      }
    });
    request.open("GET", resource);
    request.send();
  });
};

// getTodos("https://jsonplaceholder.typicode.com/todos")
//   .then((data) => {
//     console.log("Promise resolved: ", data[1]);
//   })
//   .catch((error) => {
//     console.log("Promise rejected: ", error[0]);
//   });

getTodos("todos/luigis.json")
  .then((data) => {
    console.log("Promise resolved 1: ", data[1]);
    return getTodos("todos/mario.json");
  })
  .then((data) => {
    console.log("Promise resolved 2: ", data[1]);
    return getTodos("todos/yoshi.json");
  })
  .then((data) => {
    console.log("Promise resolved 3: ", data[1]);
  })
  .catch((error) => {
    console.log("Promise rejected 4: ", error[0]);
  });