const canvas = document.querySelector('#canvas');
const ctx = canvas.getContext('2d');

canvas.width = 300;
canvas.height = 300;

canvas.addEventListener('click', (e) => {
    const coords = getCoords(e);
    console.log(coords);
});

