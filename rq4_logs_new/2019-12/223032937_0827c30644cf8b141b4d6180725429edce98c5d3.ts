import { StringUtils } from '../../util/StringUtils';

export class Student {
    private _name: string;
    private _surname: string;
    private _studentNumber: string;

    constructor(name: string, surname: string, studentNumber: string) {
        this._name = name;
        this._surname = surname;
        this._studentNumber = studentNumber;
    }

    public static emptyData(): Student {
        return new Student(null, null, null);
    }

    public emptyDataIfNull() {
        this.name = StringUtils.emptyIfNull(this.name);
        this.surname = StringUtils.emptyIfNull(this.surname);
        this.studentNumber = StringUtils.emptyIfNull(this.studentNumber);
    }

    get name(): string {
        return this._name;
    }

    set name(value: string) {
        this._name = value;
    }

    get surname(): string {
        return this._surname;
    }

    set surname(value: string) {
        this._surname = value;
    }

    get studentNumber(): string {
        return this._studentNumber;
    }

    set studentNumber(value: string) {
        this._studentNumber = value;
    }

    public isDataFilled(): boolean {
        return StringUtils.isNotEmpty(this._name)
            && StringUtils.isNotEmpty(this._surname)
            && StringUtils.isNotEmpty(this._studentNumber);
    }
}