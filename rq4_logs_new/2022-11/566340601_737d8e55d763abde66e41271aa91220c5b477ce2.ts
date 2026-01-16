// Convert string to date in format: DD/MM/YYYY
function convert(dates: any) {
    var date = new Date(dates),
        mnth = ("0" + (date.getMonth() + 1)).slice(-2),
        day = ("0" + date.getDate()).slice(-2);
    return ([date.getFullYear(), mnth, day].join("-"));
}

function getDates() {
    var date1 = new Date(convert('Wed Nov 02 2022 15:56:51 GMT+0000'));
    var date2 = new Date(convert('Wed Nov 04 2022 15:56:51 GMT+0000'));

    // Get all dates inbetween the two dates
    var dates = [];
    while (date1 <= date2) {
        dates.push(convert(date1));
        date1.setDate(date1.getDate() + 1);
    }

    console.log(dates);
    return dates;
};

module.exports = {
    convert,
    getDates
}