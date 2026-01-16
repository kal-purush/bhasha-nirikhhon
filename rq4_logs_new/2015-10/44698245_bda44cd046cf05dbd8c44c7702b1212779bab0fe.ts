/**
 * Created by kalle on 13.05.2015.
 */
import IGenericCalendarRule = require('./IGenericCalendarRule');

class GenericCalendarRule implements IGenericCalendarRule {
    private _json;

    private static _divI(n1:number, n2:number):number {
        return (n1 - (n1 % n2)) / n2;
    }

    constructor(json:string) {
        this._json = JSON.parse(json);
    }

    get yearDays():number {
        return this._json.year_days;
    }

    get seconds():number {
        return this._json.seconds;
    }

    get minutes():number {
        return this._json.minutes;
    }

    get hours():number {
        return this._json.hours;
    }

    get weekDays():number {
        return this._json.week_days;
    }

    get leapMonths():Array<number> {
        return this._json.leap_months;
    }

    get months():Array<number> {
        return this._json.months;
    }

    get leapYear():Array<number> {
        return this._json.leap_year;
    }

    get leapYearDays():number {
        return this._json.leap_year_days;
    }

    isLeapYear(year:number):boolean {
        return (year % this._json.leap_year[2] == 0 || (year % this._json.leap_year[1] != 0 && year % this._json.leap_year[0] == 0)); //TODO: modolos must be nullable and: this must be designed so. that index1 index2 ALWAYS must be % value0 = 0. Anything else does not make sense! (because it's no exception and no force then)
    }

    getLeapYearsSince(year:number, since?:number):number {
        var leap_years_until_year = 0;
        if (since)
            leap_years_until_year = this.getLeapYearsSince(since + 1);

        var leap_years_since_0 = GenericCalendarRule._divI(year - 1, this._json.leap_year[0])
            - GenericCalendarRule._divI(year - 1, this._json.leap_year[1])
            + GenericCalendarRule._divI(year - 1, this._json.leap_year[2]);

        return leap_years_since_0 - leap_years_until_year;
    }

    getLeapDaysSince(day:number, since?:number):number {
        var leap_days_since = 0;
        if (since)
            leap_days_since = this.getLeapDaysSince(since);

        var days_rule0 = this._json.leap_year[0] * this._json.year_days
            + this._json.leap_year_days - this._json.year_days;
        var days_rule1 = this._json.leap_year[1] * this._json.year_days
            - this._json.leap_year[1] / this._json.leap_year[0] * (this._json.leap_year_days - this._json.year_days);
        var days_rule2 = this._json.leap_year[2] * this._json.year_days
            + this._json.leap_year[2] / this._json.leap_year[0] * (this._json.leap_year_days - this._json.year_days)
            - this._json.leap_year[2] / this._json.leap_year[1] * (this._json.leap_year_days - this._json.year_days);

        var rule2 = GenericCalendarRule._divI(day, days_rule2);
        var rule1 = GenericCalendarRule._divI(day, days_rule1);
        var rule0 = GenericCalendarRule._divI(day, days_rule0);

        var leap_days = rule0 - rule1 + rule2;
        var day_of_year = (day - leap_days) % this._json.year_days;
        var year = GenericCalendarRule._divI(day - leap_days, this._json.year_days);

        var leap_months = this._json.leap_months;
        var months = this._json.months;
        var count_days = 0;
        if (this.isLeapYear(year + 1)) {
            for (var i = 0; i < leap_months.length; i++) {
                count_days = count_days + leap_months[i];
                leap_days = leap_days + leap_months[i] - months[i];
                if (count_days >= day_of_year)
                    break;
            }
        }

        return leap_days - leap_days_since;
    }

    getDaysSince(year:number, since?:number):number {
        if (since != undefined)
            return (year - 1 - since) * this._json.year_days + this.getLeapYearsSince(year, since) * (this._json.leapYearDays - this._json.year_days);

        return (year - 1) * this._json.year_days + this.getLeapYearsSince(year) * (this._json.leap_year_days - this._json.year_days);
    }

}
export = GenericCalendarRule;