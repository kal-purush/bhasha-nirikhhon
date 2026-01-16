import {State, Day, Week} from '../module';

interface IStateParams extends angular.ui.IStateParamsService {
    year: string;
    month: string;
    date: string;
}

@State('week', {
    url: '/week/:year/:month/:date',
    template: require('html!./week.html')
})
export class WeekController {

    selected: Day;
    year: number;
    month: number;
    date: number;
    week: Week;
    hours: number[] = [];

    constructor(
        private $stateParams: IStateParams,
        private $state: angular.ui.IStateService
    ) {
        this.init();
    }

    init() {
        for (let i = 0; i < 24; i++) {
            this.hours.push(i);
        }

        this.year = parseInt(this.$stateParams.year);
        this.month = parseInt(this.$stateParams.month);
        this.date = parseInt(this.$stateParams.date);
        this.selected = Day.create({
            year: this.year,
            month: this.month,
            date: this.date
        });

        this.week = new Week(this.year, this.month, this.selected.week);
    }

    toMonth() {
        this.$state.go('month', {
            year: this.selected.year,
            month: this.selected.month
        });
    }

    select(day: Day) {
        this.selected = day;
    }

    isSelected(day: Day) {
        if (!this.selected) {
            return false;
        }
        return Day.eq(day, this.selected);
    }

}