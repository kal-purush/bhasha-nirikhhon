let AMPM = function (hours) {
    let ampm = hours >= 12 ? "PM" : "AM";
    hours %= 12;
    hours = hours ? hours : 12;
    return hours + ampm;
};
let currentHour = new Date().getHours();
console.log(currentHour);
let changeColors = function (hours) {
    switch (true) {
        case (hours < currentHour):
            $(".description").removeClass("present future").addClass("past");
            break;
        case (hours === currentHour):
            $(".description").removeClass("future past").addClass("present");
            break;
        case (hours > currentHour):
            $(".description").removeClass("present past").addClass("future");
            break;
    }
}
let storeEvents = function () {
    localStorage.events[i] = value; 
}
let displayDailyPlanner = function () {
    let now = moment().format('dddd MMMM Do YYYY');
    $("#currentDay").text(now);
    for (let i = 9; i < 18; i++) {
        let hourlyRow =
            `<div class="row ">
        <div class="col-sm-2 hour">
                <p> ${AMPM(i)} </p>
        </div>
        <div class="col-sm-8 description"">
            <textarea placeholder="Add event here..."></textarea>
        </div>
        <div class="col-sm-2 saveBtn">
            <button class="fas fa-save"></button>
        </div>
    </div>`
        $(".container").append(hourlyRow);
        console.log(i);
        changeColors(i);
    };
};
$(".container").on("click", function (e) {
e.preventDefault();

})


displayDailyPlanner();