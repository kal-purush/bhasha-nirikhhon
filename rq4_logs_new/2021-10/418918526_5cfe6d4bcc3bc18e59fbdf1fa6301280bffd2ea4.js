var saveBtn = $(".saveBtn");
var time = $(".hour");

var currentTime = moment().format("MMMM Do YYYY h:mma");
$("#currentDay").text(currentTime);

saveBtn.on("click", function() {
  time = $(this).siblings(".hour").text();
  var plan = $(this).siblings(".description").val();

  localStorage.setItem(time, plan);
});

function hourUpdate(){
    var currentHour = moment().hours();
    console.log(currentHour)

    if (currentHour < time) {
        $(this).css("background-color", "yellow");
    }
}
