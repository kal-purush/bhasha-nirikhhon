const url = "https://s3.amazonaws.com/dolartoday/data.json";

fetchData(url);

async function fetchData(url) {
  try {
    const response = await fetch(url);
    const data = await response.json();
    renderData(data);
  } catch (error) {
    console.warn(error);
  }
}

function renderData(data) {
  const card = document.querySelector(".container");

  for (const key in data.USD) {
    // console.log(`${key} = ${data.USD[key]}`);
    const itemCard = document.createElement("div");
    itemCard.className = "item-card";
    card.append(itemCard);

    const itemList = document.createElement("li");
    itemList.className = "actual-value";
    itemList.textContent = data.USD[key];
    itemCard.append(itemList);
  }
}