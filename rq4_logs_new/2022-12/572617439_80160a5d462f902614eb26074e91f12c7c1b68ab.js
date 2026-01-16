async function Img() {
  const imgContainer = document.querySelector(".container");
  const limit = document.getElementsByName("input");
  const API_URL = `https://api.thedogapi.com/v1/images/search?limit=${limit[0].value}&api_key=live_c6lMhShvb9iIc1o1FDZZf51xDv6p0rfqrk5i0fkKIjKBOoRqFc81miJBsnVqMGBp`;

  imgContainer.replaceChildren(
    document.createRange().createContextualFragment(``)
  );

  const res = await fetch(API_URL);
  const data = await res.json();
  for (let i = 0; i < limit[0].value; i++) {
    const img = document
      .createRange()
      .createContextualFragment(`<img src=${data[i].url} alt=""/>`);
    console.log(img);
    imgContainer.append(img);
  }
}