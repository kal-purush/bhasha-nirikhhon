document.getElementById("toTop").addEventListener("click", () => {
  // const mapHeight = document.querySelector(".mapSpace").offsetHeight;
  // console.log("map height", mapHeight);
  // const article = document.querySelector(".article");
  // console.log(article);

  // document.body.scrollTop = 0;
  // document.documentElement.scrollTop = 0;

  window.scroll({
    top: 0,
    // left: 0,
    behaviour: "smooth"
  });
});