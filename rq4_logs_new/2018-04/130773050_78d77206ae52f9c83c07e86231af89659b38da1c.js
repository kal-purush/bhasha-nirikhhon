function quickSun() {

    var now = new Date();
    var today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    var lastSunday = new Date(today.setDate(today.getDate() - today.getDay()));
    
    var lastlastSunday = clone(lastSunday,false);
    lastlastSunday.setDate(lastlastSunday.getDate() - 7);
    
    
    setDateTimes(lastlastSunday, "12:00am", lastSunday, "12:00am");
}

function quickWeek() {
    var now = new Date();
    var oneWeekAgo = new Date();
    oneWeekAgo.setDate(oneWeekAgo.getDate() - 7)
    setDateTimes(oneWeekAgo, oneWeekAgo, now, now);
}

function quickMonth() {

    var now = new Date();
    var oneMonthAgo = new Date();
    oneMonthAgo.setDate(oneMonthAgo.getDate() - 30)
    setDateTimes(oneMonthAgo, oneMonthAgo, now, now);
}


function setDateTimes(startDate, startTime, endDate, endTime) {
    $('#startDate').datepicker("setDate", startDate);
    $('#startTime').timepicker("setTime", startTime);

    $('#endDate').datepicker("setDate", endDate);
    $('#endTime').timepicker("setTime", endTime);

}