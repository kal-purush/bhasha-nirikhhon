const location1 = document.querySelector('.location1');
const location2 = document.querySelector('.location2');
const location3 = document.querySelector('.location3');
const location4 = document.querySelector('.location4');
const location5 = document.querySelector('.location5');
const buss = document.querySelector('#buss');



// var animTime = 0;
// function addAutoAnim(propTime) {
//     setInterval(function () {
//         buss.classList.add('anim');
//     // alert(propTime);
//     }, propTime);
// }

location1.addEventListener('click', () => {
    buss.classList.add('location1');
    buss.classList.remove('anim');
    buss.classList.remove('location2');
    buss.classList.remove('location3');
    buss.classList.remove('location4');
    buss.classList.remove('location5');
    // addAutoAnim(5000);
});
location2.addEventListener('click', () => {
    buss.classList.remove('anim');
    buss.classList.add('location2');
    buss.classList.remove('location1');
    buss.classList.remove('location3');
    buss.classList.remove('location4');
    buss.classList.remove('location5');
    // addAutoAnim(5000);
});

location3.addEventListener('click', () => {
    buss.classList.remove('location1');
    buss.classList.remove('anim');
    buss.classList.remove('location2');
    buss.classList.add('location3');
    buss.classList.remove('location4');
    buss.classList.remove('location5');
    // addAutoAnim(5000);
});

location4.addEventListener('click', () => {
    buss.classList.remove('location1');
    buss.classList.remove('anim');
    buss.classList.remove('location2');
    buss.classList.remove('location3');
    buss.classList.add('location4');
    buss.classList.remove('location5');
    // addAutoAnim(5000);
});

location5.addEventListener('click', () => {
    buss.classList.remove('location1');
    buss.classList.remove('anim');
    buss.classList.remove('location2');
    buss.classList.remove('location3');
    buss.classList.remove('location4');
    buss.classList.add('location5');
    // addAutoAnim(5000);
});