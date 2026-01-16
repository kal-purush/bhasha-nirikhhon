setInterval(console.log('farts'), 1000);
const hourHand = document.querySelector('hour-hand');
const minuteHand = document.querySelector('minute-hand');
const secondHand = document.querySelector('second-hand');

const findTime = () => {
  const date = new Date();

  const seconds = date.getSeconds();
  const secondsDegrees = ((seconds / 60) * 360) + 90;

  const hours = date.getHours();
  const hoursDegrees = ((hours / 60) * 360) + 90;

  const minutes = date.getMinutes();
  const minutesDegrees = ((minutes / 60) * 360) + 90;

  console.log(secondsDegrees);
  console.log(minutesDegrees);
  console.log(hoursDegrees);

}

setInterval(findTime, 1000);