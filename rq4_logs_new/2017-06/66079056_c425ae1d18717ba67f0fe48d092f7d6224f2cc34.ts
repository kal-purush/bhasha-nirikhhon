export class DosagePeriod {
    type: string;
    mapping: string;
    iteration: number;
    beginDate: Date;
    endDate: Date;

    constructor (type: string, mapping: string, iteration: number, beginDate: Date, endDate: Date) {
        this.type = type;
        this.mapping = mapping;
        this.iteration = iteration;
        this.beginDate = beginDate;
        this.endDate = endDate;
    }

    public getType(): string {
        return this.type;
    }

    public getMapping(): string {
        return this.mapping;
    }

    public getIteration(): number {
        return this.iteration;
    }

    public getBeginDate(): Date {
        return this.beginDate;
    }

    public getEndDate(): Date {
        return this.endDate;
    }
}