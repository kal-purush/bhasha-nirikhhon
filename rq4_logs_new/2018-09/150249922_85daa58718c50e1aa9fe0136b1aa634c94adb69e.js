function divider(array) {
  var result = array.map(function(element) {
    if (typeof element === "string") {
      var reg = /[\d|,|\\|\/|.|e|E|\+]+/g;
      var matches = reg.exec(element);
      if (matches) {
        var resObj = {};
        resObj.name = element.slice(0, matches.index - 1);
        resObj.count = matches[0];
        resObj.measure = element.slice(
          matches.index + matches[0].length + 1,
          element.length
        );
        return resObj;
      } else {
        return element;
      }
    } else {
      return element;
    }
  });

  return result;
}

function getData() {
  fetch("http://localhost:3000/itemListElement")
    .then(res => {
      return res.json();
    })
    .then(data =>
      fetch("http://localhost:3000/test", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify(parseData(data))
      })
    );
}

function parseData(data) {
  data.forEach((element, index) => {
    element.id = index;
    element.recipeIngredient = [...divider(element.recipeIngredient)];
  });
  return data;
}

getData();