document.getElementById("newImage").onclick = function () {
  // document.getElementById('wallpaper').va
  const src = "https://source.unsplash.com/random/3840x2160";
  console.log(src);
  // let img = document.createElement("img");
  // img.src = src;
  const wallpaper = document.getElementById("wallpaper");
  wallpaper.style.background = `url(${src})`;
  // wallpaper.style.background = `#f3f3f3`;
  // document.body.replaceChild();
};