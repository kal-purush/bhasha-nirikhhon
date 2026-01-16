import * as bluebird from 'bluebird';
import moment from 'moment';

export interface GuardSchedule {
    id: number;
    status: string;
    date: moment.Moment;

}
export interface GuardSleep {
    id: number;
    minutes: number;
}
export class Guards {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));

    public static findSleepMethodTwo = async (): Promise<number> => {
        let schedule = await Guards.getData();
        let mapped: { [id: string]: { [id: string]: number } } = {};
        let start: moment.Moment = moment();
        schedule.forEach((e) => {
            if (!mapped[e.id.toString()]) {
                mapped[e.id.toString()] = {};
            }
            if (e.status.indexOf("falls asleep") > -1) {
                start = e.date;
            }
            if (e.status.indexOf("wakes up") > -1) {
                let minS = start.get("minute");
                let minE = e.date.get("minute");
                for (; minS < minE; minS++) {
                    if (!mapped[e.id.toString()][minS.toString()]) {
                        mapped[e.id.toString()][minS.toString()] = 0;
                    }
                    mapped[e.id.toString()][minS.toString()]++;
                }
            }
        });

        let minMap: { [id: string]: { id: string, count: number } } = {};
        Object.entries(mapped).forEach(e => {
            let m = Object.entries(e[1]).reduce((max, c) => c[1] > max[1] ? c : max, ["", 0]);
            if (!minMap[m[0]] || m[1] > minMap[m[0]].count) {
                minMap[m[0]] = { id: e[0], count: m[1] };
            }
        });

        let maxG = Object.entries(minMap).reduce((max, c) => c[1].count > max[1].count ? c : max, ["", { id: "", count: 0 }]);
        return Number(maxG[1].id) * Number(maxG[0]);
    }

    public static findSleepMethodOne = async (): Promise<number> => {
        let schedule = await Guards.getData();
        let totalSleep = Guards.getGuardSleep(schedule);
        let max = totalSleep.reduce((max, e) => e.minutes > max.minutes ? e : max, { id: 0, minutes: 0 });


        let times = schedule.filter((x) => x.id === max.id);
        let minutes: { [id: string]: number } = {};
        let start: moment.Moment = moment();
        times.forEach((e) => {
            if (e.status.indexOf("falls asleep") > -1) {
                start = e.date;
            }
            if (e.status.indexOf("wakes up") > -1) {
                let minS = start.get("minute");
                let minE = e.date.get("minute");
                for (; minS < minE; minS++) {
                    if (!minutes[minS.toString()]) {
                        minutes[minS.toString()] = 0;
                    }
                    minutes[minS.toString()]++;
                }
            }
        });
        let maxMin = Object.entries(minutes).reduce((max, e) => e[1] > max[1] ? e : max, ["", 0]);

        return Number(max.id) * Number(maxMin[0]);
    }

    public static getGuardSleep = (schedule: GuardSchedule[]): GuardSleep[] => {
        let guards: { [id: string]: number[] } = {};
        let start: moment.Moment = moment();
        schedule.forEach((entry) => {
            if (!guards[entry.id.toString()]) {
                guards[entry.id.toString()] = [];
            }
            if (entry.status.indexOf("falls asleep") > -1) {
                start = entry.date;
            }
            if (entry.status.indexOf("wakes up") > -1) {
                guards[entry.id.toString()].push(entry.date.diff(start, "minutes"));
            }
        });
        return Object.entries(guards).map((e) => {
            return {
                id: Number(e[0]),
                minutes: e[1].reduce((total, cur) => total + cur, 0)
            };
        });
    }

    public static getData = async (): Promise<GuardSchedule[]> => {
        let raw: string = await Guards.fs.readFileAsync("input/dayFour.txt", { encoding: "utf8" });
        let split = raw.split("\n");
        let schedule: GuardSchedule[] = split.map((entry) => {

            let parts = entry.split(']');
            let status = parts[1].trim();
            let date = moment(parts[0].replace("[", ""), "YYYY-MM-DD HH:mm");

            return {
                id: 0,
                date,
                status
            }
        });
        schedule.sort((one, two) => {
            return one.date.diff(two.date);
        });

        let id = -1;
        schedule.forEach((v) => {
            if (v.status.indexOf("begins shift") > -1) {
                id = Number(v.status.split(' ')[1].replace('#', ''));
            }
            v.id = id;
        });
        let string = schedule.map((x) => {
            return x.date.format() + "   " + x.id + "   " + x.status;
        }).join('\n');
        await Guards.fs.writeFileAsync("input/dayFourCleaned.txt", string);
        return schedule;
    }
}