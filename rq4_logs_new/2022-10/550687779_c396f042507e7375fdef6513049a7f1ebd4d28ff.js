import cropImage from "./index.js";

cropImage('./images/1.jpg', function(cropped) {
    console.log(cropped)
})