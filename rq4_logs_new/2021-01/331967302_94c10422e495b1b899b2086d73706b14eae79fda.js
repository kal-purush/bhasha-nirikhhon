const dayDisplay = document.querySelector(".days .number");
const hourDisplay = document.querySelector(".hours .number");
const minuteDisplay = document.querySelector(".minutes .number");
const secondDisplay = document.querySelector(".seconds .number");

const countdownContainer = document.querySelector(".countdown-container");

const endDate = new Date("July 31 2020 17:00:00");
let saleEnded = false;

const updateTimer = () => {
  let currentDate = new Date();
  let difference = endDate.getTime() - currentDate.getTime();

  if (difference <= 1000) {
    saleEnded = true;
  }

  const second = 1000;
  const minute = second * 60;
  const hour = minute * 60;
  const day = hour * 24;

  let newDay = Math.floor(difference / day);
  let newHour = Math.floor((difference % day) / hour);
  let newMiute = Math.floor((difference % hour) / minute);
  let newSecond = Math.floor((difference % minute) / second);

  dayDisplay.innerText = newDay < 10 ? "0" + newDay : newDay;
  hourDisplay.innerText = newHour < 10 ? "0" + newHour : newHour;
  minuteDisplay.innerText = newMiute < 10 ? "0" + newMiute : newMiute;
  secondDisplay.innerText = newSecond < 10 ? "0" + newSecond : newSecond;
};

setInterval(() => {
  if (!saleEnded) {
    updateTimer();
  } else {
    countdownContainer.style.display = "none";
  }
}, 1000);