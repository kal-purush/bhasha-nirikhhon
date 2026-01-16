import { Comparable } from "./Comparable";
import moment from 'moment';
export class DateComparable implements Comparable<moment.Moment> {

    private date: moment.Moment;
    public constructor(date: moment.Moment) {
        this.date = date;
    }
    public static parse(dateAsStr: string, format?: string): any {
        return new DateComparable(moment(dateAsStr, format));
    }
    lessThan(toCompare: Comparable<moment.Moment>): boolean {
        throw new Error("Method not implemented.");
    }
    greaterThan(toCompare: Comparable<moment.Moment>): boolean {
        throw new Error("Method not implemented.");
    }
    equals(toCompare: Comparable<moment.Moment>): boolean {
        return this.getTimestamp() === toCompare.getValue().valueOf();
    }
    getValue(): moment.Moment {
        return this.date;
    }

    private getTimestamp(): number {
        return this.getValue().valueOf();
    }
}