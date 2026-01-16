const fullName = document.querySelector('#fullname');
const jobTitle = document.querySelector('#jobtitle');
const btn = document.querySelector('#btn');
const form = document.querySelector('#form');
const dropzone = document.querySelector('.dropzone');



const prgh1 = document.querySelector('.prgh1');
fullName.addEventListener('change', (e) => {
    return prgh1.textContent = e.target.value;
});

const prgh2 = document.querySelector('.prgh2');
jobTitle.addEventListener('click', (e) => {
    return prgh2.textContent = e.target.value;
});

form.addEventListener('submit', (e) => {
    e.preventDefault();
    const prgh3 = document.querySelector('.prgh3');
    return prgh3.innerHTML = prgh1.textContent + `</br>` + prgh2.textContent;
});

const prgh4 = document.querySelector('.prgh4');
window.addEventListener("load", (e) => {
    window.alert('The page loaded successfully!');
    let text = "The page loaded successfully!";
    return prgh4.innerHTML = text;
});

let dragged;
const prgh5 = document.querySelector('.prgh5');
prgh5.addEventListener("drag", (e) => {
    console.log("dragging");
    return dragged = e.target;
});

const target = document.getElementById("droptarget");
target.addEventListener("dragover", (e) => {
    // prevent default to allow drop
    e.preventDefault();
}, false);

dropzone.addEventListener('drop', (e) => {
    e.preventDefault();

    if (e.target.classList.contains("dropzone")) {
        return e.target.appendChild(dragged);
    }
});

const prgh6 = document.querySelector('.prgh6');
prgh6.addEventListener('dblclick', (e) => {
    return prgh6.classList.toggle('large');
});

const network = document.querySelector('.network');
window.addEventListener('online', (e) => {
    console.log("You are now connected to the network.");
    network.classList.remove('offline');
    return network.classList.add('online');
});


window.addEventListener('offline', (e) => {
    console.log("The network connection has been lost.");
    network.classList.remove('online');
    return network.classList.add('offline');
});

