const images = [
    "1.jpg", "2.jpg", "3.jpg" 
];
const randomN = Math.floor(Math.random() * images.length);
const chosenImage = images[randomN];

const bgImage = document.createElement("style");

bgImage.innerText = `html {
    background: url(img/${chosenImage}) no-repeat center fixed;
    webkit-background-size: cover;
    -moz-background-size: cover;
    -o-background-size: cover;
    background-size: cover;
}`;


document.body.appendChild(bgImage);
