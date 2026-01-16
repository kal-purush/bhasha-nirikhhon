interface Day {
    date: number;
    day: string;
    otherMonth?: boolean;
    today?: boolean;
}

class Week {
    days: Day[] = [];
    static WeekLabels = '日 月 火 水 木 金 土'.split(' ');

    constructor(
        public year,
        public month,
        public week
    ) {
        this.init();
    }

    init() {
        let day = new Date(this.year, this.month, 1);
        day.setDate(day.getDate() - day.getDay());
        day.setDate(day.getDate() + 7 * (this.week));
        let now = new Date();

        for (let i = 0; i < 7; i++) {
            this.days.push({
                date: day.getDate(),
                day: Week.WeekLabels[day.getDay()],
                otherMonth: this.month !== day.getMonth(),
                today: (
                    now.getFullYear() === day.getFullYear() &&
                    now.getMonth() === day.getMonth() &&
                    now.getDate() === day.getDate()
                )
            });
            day.setDate(day.getDate() + 1);
        }

    }
}

class Month {
    weeks: Week[] = [];
    days: Day[] = [];

    constructor(
        public year: number,
        public month: number
    ) {
        this.init();
    }

    init() {
        let now = new Date();
        this.year = now.getFullYear();
        this.month = now.getMonth();

        let day = new Date(this.year, this.month, 1);

        while (day.getMonth() == this.month) {
            this.days.push({
                date: day.getDate(),
                day: Week.WeekLabels[day.getDay()]
            });
            day.setDate(day.getDate() + 1);
        }

        let weekLength = Math.round(this.days.length / 7 + 1);
        for (let i = 0; i < weekLength; i++) {
            this.weeks.push(new Week(this.year, this.month, i));
        }

    }
}

export class MonthController {

    year: number;
    month: number;
    days: Day[];
    weeks: Week[];
    weekLabels: string[];

    constructor() {
        this.init();
    }


    selectDate(day: Day) {
        console.log(day);
    }

    init() {
        this.weekLabels = Week.WeekLabels;
        let now = new Date();
        this.year = now.getFullYear();
        this.month = now.getMonth();
        let month = new Month(this.year, this.month);
        this.days = month.days;
        this.weeks = month.weeks;
    }
}