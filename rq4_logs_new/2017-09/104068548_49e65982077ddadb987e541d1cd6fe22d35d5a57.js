(function (testMode) {

    if (testMode) {
        console.log('Test mode ON');
    }

    var weekday = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

    var bankHolidays = [
        getHumanDate(2017, 1, 2),
        getHumanDate(2017, 1, 9),
        getHumanDate(2017, 3, 8),
        getHumanDate(2017, 4, 17),
        getHumanDate(2017, 5, 1),
        getHumanDate(2017, 5, 2),
        getHumanDate(2017, 5, 9),
        getHumanDate(2017, 6, 5),
        getHumanDate(2017, 6, 28),
        getHumanDate(2017, 8, 24),
        getHumanDate(2017, 10, 16)
    ];

    var holidaysTimestamp = bankHolidays.map(function(d){return d.getTime()});

    function getHumanDate(y, m, d, h, min, s) {
        if (typeof m == 'undefined') m = 1;
        if (typeof d == 'undefined') d = 1;
        if (typeof h == 'undefined') h = 0;
        if (typeof min == 'undefined') min = 0;
        if (typeof s == 'undefined') s = 0;
        return new Date(y, m - 1, d, h, min, s);
    }

    function getData(attr) {
        var selector = function (s) {
            return $("input[ng-model='" + s + "']").val()
        }
        var values = {
            'expected': (testMode ? '168h 0min (96h 0min)' : selector('attendance.BothExpected')),
            'actual': (testMode ? '95h 13min' : selector('attendance.Actual')),
            'now': (testMode ? getHumanDate(2017, 10, 15, 12) : new Date())
        };

        return values[attr];
    }

    function isBankHoliday(date) {
        return holidaysTimestamp.indexOf(date.getTime()) !== -1;
    }

    function isWorkingDay(date) {
        return weekday[date.getDay()] !== 'Saturday'
            && weekday[date.getDay()] !== 'Sunday'
            && !isBankHoliday(date);
    }

    function getDaysInMonth(date) {
        var d = getLastDateInMonth(date);
        return d.getDate();
    }

    function getLastDateInMonth(date) {
        return new Date(date.getFullYear(), date.getMonth() + 1, 0);
    }

    function getEndOfWorkingDay(date) {
        return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 18);
    }

    function getWorkingMinsLeft(date) {
        var end = getEndOfWorkingDay(date);

        var dateMs = date.getTime();
        //console.log('date time:', date, dateMs);
        var endMs = end.getTime();
        //console.log('end time:', end, endMs);
        var mins = Math.round((endMs - dateMs) / 1000 / 60);
        //console.log('Mins diff', mins);
        return mins > 0 ? mins : 0;
    }

    function getWorkingDaysCount(date) {
        var dateDayN = date.getDate();
        var lastDayN = getDaysInMonth(date);
        var businessDays = 0;
        while (dateDayN++ < lastDayN) {
            var d = new Date(date.getFullYear(), date.getMonth(), dateDayN);
            if (isWorkingDay(d)) {
                //console.log(d);
                businessDays++;
            }
        }
        return businessDays;
    }

    function getMinutes(attr) {

        var val = getData(attr);

        if (val) {
            var arr = val.split(' ');

            var h = arr[0].replace('h', '');
            h = h.replace('hour', '');
            h = h.replace('hours', '');
            h = parseInt(h, 10);

            var m = arr[1].replace('min', '');
            m = m.replace('mins', '');
            m = m.replace('m', '');
            m = parseInt(m, 10);

            return h * 60 + m;
        }
        else {
            return 0;
        }
    }

    function formatMins(mins) {
        var r = '';
        var h = Math.trunc(mins / 60);
        var m = mins - h * 60;

        if (h > 0) {
            r = h + 'h';
        }
        if (m > 0) {
            if (r !== '') {
                r += ' ';
            }
            r += Math.round(m) + 'm';
        }

        if (r === '') {
            r = 'unknown';
        }

        return r;
    }

    var expectedM = getMinutes('expected');
    var actualM = getMinutes('actual');
    var nowDate = getData('now');

    var leftTotalM = expectedM - actualM;
    var leftTodayM = getWorkingMinsLeft(nowDate);
    var workingDays = getWorkingDaysCount(nowDate);
    var leftMPerDayM = workingDays > 0 ? (leftTotalM - leftTodayM) / workingDays : 0;

    return {
        'Left total': formatMins(leftTotalM) + ' (' + formatMins(leftMPerDayM) + ' / ' + workingDays + ' days)',
        'Left today': formatMins(leftTodayM)
    };

})(false);