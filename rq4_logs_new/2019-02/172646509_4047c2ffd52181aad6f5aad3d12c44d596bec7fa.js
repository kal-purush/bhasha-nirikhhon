let div = document.getElementById('div')
let x1 = 0, y1 = 0, x2 = 0, y2 = 0;

// calculate the position and size of the div
const calculate = () => {
    let x3 = Math.min(x1,x2);
    let x4 = Math.max(x1,x2);
    let y3 = Math.min(y1,y2);
    let y4 = Math.max(y1,y2);
    div.style.left = x3 + 'px';
    div.style.top = y3 + 'px';
    div.style.width = x4 - x3 + 'px';
    div.style.height = y4 - y3 + 'px';
}

// make the div visible and save the starting x and y coordinates
onmousedown = event => {
    div.hidden = 0;
    x1 = event.clientX;
    y1 = event.clientY;
    calculate();
}

// get the new x and y coordinates and calculate width and height
onmousemove = event => {
    x2 = event.clientX;
    y2 = event.clientY;
    calculate();
}

// make the div hidden again and draw the cropped image area
onmouseup = event => {
    div.hidden = 1;
    let left = parseInt(div.style.left, 10)
    let top = parseInt(div.style.top, 10)
    let width = parseInt(div.style.width, 10)
    let height = parseInt(div.style.height, 10)
    if (width != height) {
        draw(left, top, width, height);
    }
}