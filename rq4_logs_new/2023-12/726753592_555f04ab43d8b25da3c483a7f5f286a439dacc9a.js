let logo = document.querySelector(".logo");

logo.addEventListener("click", (e) => {
  location.reload();
});



let son1 = document.querySelector('.number')


let raqam = new Date()

let qolganKun = 31 - raqam.getDate();

son1.innerHTML = qolganKun