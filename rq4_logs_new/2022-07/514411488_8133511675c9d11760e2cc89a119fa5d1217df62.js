//Start of the 'click' event listener section

//Defining the array of colors we'll be using for the background as well as the 
//index variable which will help us move through the array
const backgroundColors=['snow','royalblue','lightgreen','orangered'];
let index=0;

//Grabbing all the needed elements from the DOM
const btnBox=document.querySelector('.btn-box');
const currentColor=document.querySelector('#bg-color');
const nextColor=document.querySelector('#next-color');
const colorChangeBtn=btnBox.querySelector('button');

//Setting the initial values for all the elements
btnBox.style.backgroundColor=backgroundColors[0];
currentColor.innerText=backgroundColors[0];
nextColor.innerText=backgroundColors[1];

//Adding a 'click" event listener on the button
colorChangeBtn.addEventListener('click', () => {
    index++;
    if(index===backgroundColors.length)
    {
        index=0;
    }

    btnBox.style.backgroundColor=backgroundColors[index];
    currentColor.innerText=backgroundColors[index];

    if(index+1===backgroundColors.length)
    {
        nextColor.innerText=backgroundColors[0];
    }
    else
    {
        nextColor.innerText=backgroundColors[index+1];
    }
});

//End of the 'click' event listener section

//Start of the 'resize' event listener section

//Grabbing all the needed elements from the DOM
const widthSpan=document.querySelector('#width-id');
const heightSpan=document.querySelector('#height-id');

//Setting the initial width and height values
widthSpan.innerText=window.innerWidth;
heightSpan.innerText=window.innerHeight;

//Adding a 'resize" event listener on the window
window.addEventListener('resize', () => {
    widthSpan.innerText=window.innerWidth;
    heightSpan.innerText=window.innerHeight;
});

//Another approach I didn't choose because I found it more
//complex
/*
window.addEventListener('resize', event => {
    widthSpan.innerText=event.currentTarget.innerWidth;
    heightSpan.innerText=event.currentTarget.innerHeight;
});
*/

//End of the 'resize' event listener section

//Start of the 'mousemove' event listener section

//Grabbing all the needed elements from the DOM
const mouseDiv=document.querySelector('.mouse-box');
const countSpan=document.querySelector('#mouse-id');

//Setting the initial values
let count=0;
countSpan.innerText=count;

//Adding a 'mousemove' event listener on the mouse-box div
mouseDiv.addEventListener('mousemove', () => {
    count++;
    countSpan.innerText=count;
});

//End of the 'mousemove' event listener section