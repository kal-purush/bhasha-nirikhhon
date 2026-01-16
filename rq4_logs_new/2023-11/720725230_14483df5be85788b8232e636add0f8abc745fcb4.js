const button = document.getElementById('click-button');  
const page = document.getElementById('page');
button.addEventListener('click', (event) => {
    console.log("user clicked the button");
    console.log(event);
    page.style.backgroundColor = 'pink';
});

