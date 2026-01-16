var addToSchedule= document.querySelector('#add-to-schedule');
var scheduleItem= document.querySelector("#item");
// var schedule= document.querySelector("#schedule");

addToSchedule.addEventListener('submit', function (event) {
    event.preventDefault();
        if (scheduleItem.value.length < 1) return;
        schedule.innerHTML += '<li>' + scheduleItem.value + '</li>';
        localStorage.setItem('scheduleItem', schedule.innerHTML);
    }, false);

    var saved = localStorage.getItem('scheduleItem');

// If there are any saved items, update our list
if (saved) {
    schedule.innerHTML = saved;
}
console.log(saved)

// sets current time
$(document).ready(function () {
    var today= moment();
    $("#currentDay").text(today.format("dddd MMMM Do YYYY, h:mm a")) ;

    var hours=today.hours();
    console.log(hours);
})

