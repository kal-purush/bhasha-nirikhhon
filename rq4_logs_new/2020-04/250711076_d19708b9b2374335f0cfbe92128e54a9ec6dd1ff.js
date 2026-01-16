let startX, startWidth;
startWidth = JSON.parse(localStorage.getItem('scalable-width')) || getScalableDivWidth();
document.querySelector('.scalable').style.width = startWidth + 'px';

document.querySelector('.separator').addEventListener('mousedown', startDrag);

function startDrag(event) {
    startX = event.clientX;
    startWidth = getScalableDivWidth();

    document.documentElement.addEventListener('mousemove', onDrag);
    document.documentElement.addEventListener('mouseup', stopDrag);
}

function onDrag(event) {
    let newWidth = startWidth + event.clientX - startX;
    document.querySelector('.scalable').style.width = newWidth + 'px';
}

function stopDrag(event) {
    localStorage.setItem('scalable-width', JSON.stringify(getScalableDivWidth()));
    document.documentElement.removeEventListener('mousemove', onDrag);
    document.documentElement.removeEventListener('mouseup', stopDrag);
}

function getScalableDivWidth() {
    return parseInt(window.getComputedStyle(document.querySelector('.scalable')).width, 10);
}