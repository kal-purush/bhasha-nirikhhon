"use strict";

const film = document.getElementById("film");
const res = document.querySelector(".res");

const getData = (url) => {
  fetch(url)
    .then((response) => response.json())
    .then((data) => {
      data.forEach(function (item) {
        let option = document.createElement("option");
        option.innerHTML = item.name;
        option.value = item.name;
        film.append(option);
      });
      const result = function () {
        res.innerHTML = "";
        let select = data[event.target.options.selectedIndex];

        for (let key in select) {
          let newSelect = JSON.parse(JSON.stringify(select), (key, value) =>
            key === "photo" ? "<img src= " + select.photo + " >" : value
          );

          let div = document.createElement("div");
          div.id = "div";
          div.innerHTML = key + ": " + newSelect[key];
          res.append(div);
        }
      };

      film.addEventListener("change", result);
    });
  //   .catch((error) => {
  //     console.log(error);
  //   });
};

getData("dbHeroes.json");