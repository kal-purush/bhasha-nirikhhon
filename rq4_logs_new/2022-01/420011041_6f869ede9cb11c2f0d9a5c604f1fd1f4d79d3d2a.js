function wResize() {
  let w = window.getComputedStyle($(".out_3d")).width;
  w = w.replace("px", "") / 2;
  for (let i = 0; i < $All(".inner_3d").length; i++) {
    $All(".inner_3d")[i].style.transform = `rotateY(calc(var(--i) * 90deg))translateZ(${w}px)`;
  }
}

function reY() {
  let maxH = 0;
  let img = $All(".inner_3d>img");
  for (let i = 0; i < img.length; i++) {
    let imgH = window.getComputedStyle(img[i]).height.replace("px", "");
    if (imgH > maxH) maxH = imgH;
  }
  let y = 0;
  y = (window.getComputedStyle($(".pic_3d")).height.replace("px", "") - maxH) / 2;
  $(".d3_position").style.top = `${y}px`
  // pre.style.top = `${maxH}px`;
  // next.style.top = `${maxH}px`;
}

let yy = 0;

function turn(e) {
  if (this.id == "pre") {
    yy -= 90;
  } else {
    yy += 90;
  }
  console.log(e);
  $(".middle_3d").style.transform = `rotateY(${yy}deg)`;
  e.stopPropagation();
}

function nothing(e) {
  e.stopPropagation();
}

function d3_displayNone() {
  $(".pic_3d").style.visibility = "hidden";
  $(".pic_3d").style.zIndex = "-10"
}


wResize();
window.addEventListener("load", function () {
  reY();
  window.addEventListener("resize", wResize, false)
  window.addEventListener("resize", reY, false)

  next.addEventListener("click", turn, false)
  pre.addEventListener("click", turn, false)

  for (let i = 0; i < $All(".pic img").length; i++) {
    $All(".pic img")[i].addEventListener("click", function () {
      $(".pic_3d").style.visibility = "visible";
      $(".pic_3d").style.zIndex = "10"
    }, false)

  }
  $(".close_3d").addEventListener("click", wResize, false)
  $(".close_3d").addEventListener("click", d3_displayNone, false)
  $(".pic_3d").addEventListener("click", d3_displayNone, false)
  for (let i = 0; i < $All(".inner_3d>img").length; i++) {
    $All(".inner_3d>img")[0].addEventListener("click", nothing, false)
  }
}, false)