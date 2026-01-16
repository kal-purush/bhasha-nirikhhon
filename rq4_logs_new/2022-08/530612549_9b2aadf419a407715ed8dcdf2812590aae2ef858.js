let day = 0;
let intervalID = setInterval(changeDay, 100);
function changeDay () {
  day += 1;
  day %= 30;
  document.querySelector('#day').innerHTML = `Day ` + day;
}