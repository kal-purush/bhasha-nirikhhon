import {Component} from '../module';
import {SelectDay, Day, Week, Month} from '../../share/calender';

@Component('yearMonth', {
    template: require('html!./year-month.html'),
    bindings: {
        year: '=',
        month: '=',
        select: '&',
        selectmonth: '&',
        selected: '='
    },
})
export class YearMonthComponent {

    year: number;
    month: number;
    days: Day[];
    weeks: Week[];
    weekLabels: string[];
    selected: Day;

    select: SelectDay;
    selectmonth: SelectDay;

    constructor(private $scope: angular.IScope) {
        this.init();
    }

    _select(day: Day) {
        this.select({
            $day: day
        })
    }

    _selectMonth() {
        console.log('selectMonth', this.selectmonth);
        this.selectmonth({
            $day: this.days[0]
        });
    }

    isIncludeMonth(day: Day) {
        return this.year === day.year && this.month === day.month;
    }

    isSelected(day: Day) {
        if (!this.selected) {
            return false;
        }
        return Day.eq(day, this.selected);
    }

    init() {
        this.weekLabels = Week.WeekLabels;

        let reflesh = () => {
            let month = new Month(this.year, this.month - 1);
            this.days = month.days;
            this.weeks = month.weeks;
        }

        this.$scope.$watch(() => {
            return this.month + this.year * 100;
        }, reflesh);

    }

}