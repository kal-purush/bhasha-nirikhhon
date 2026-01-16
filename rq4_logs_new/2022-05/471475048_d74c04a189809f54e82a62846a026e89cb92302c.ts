import OptionParameter from "../OptionParameter";

export default class DateOptionParameter extends OptionParameter<Date> {
    public get Max(): string | number | undefined {
        return this.max ? DateOptionParameter.formatDate(this.max) : undefined
    }
    public get Min(): string | undefined {
        return this.min ? DateOptionParameter.formatDate(this.min) : undefined
    }

    public type = 'date';
    fromRaw(rawValue: string): Date {
        return new Date(rawValue)
    }

    public static formatDate(date: Date) {
        return date.toISOString().slice(0, 10)
    }
}