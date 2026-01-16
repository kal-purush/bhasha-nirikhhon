const appTl = new TimelineLite();
const imageTl = new TimelineLite();
let imgPrefix = "./assets/"
let imgList = ["cheese.jpg", "pink&blue.jpg", "yellow.jpg", "green&yellow.jpg" , "green&white.jpg", "brown&orange.jpg"] 


let portrait = document.getElementById("portrait");
let wrapper = document.getElementsByClassName("image-wrapper")
let heartBtn = document.getElementById("heart-btn");
let commentBtn = document.getElementById("comment-btn");
let prevBtn = document.getElementById("prev-btn");
let nextBtn = document.getElementById("next-btn");
let index = 0
portrait.src = imgPrefix + imgList[index]

imageTl
.from(portrait, 1.2, {scale: 1.3, ease: Power3.ease}, .2)

appTl
.from(wrapper, 1.5, {y: -1280, ease: Power3.easeOut}, .2)
.from(prevBtn, 1.2, {x: -1280, ease: Power3.ease}, .2)
.from(nextBtn, 1.2, {x: 1280, ease: Power3.ease}, .2)
.staggerFrom([heartBtn, commentBtn], .5, {y:1280, ease: Power3.easeOut, delay: 0}, .10)

nextBtn.addEventListener("click", () => {
    index = (index === imgList.length - 1) ? 0 : index + 1;
    portrait.src = imgPrefix + imgList[index]
    imageTl.restart()
})

prevBtn.addEventListener("click", () => {
    index = (index === 0) ? imgList.length - 1 : index - 1
    portrait.src = imgPrefix + imgList[index]
    imageTl.restart()
})