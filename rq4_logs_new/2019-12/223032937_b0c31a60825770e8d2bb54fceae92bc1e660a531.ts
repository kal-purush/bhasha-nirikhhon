import {Student} from './Student';
import {PaymentDuration} from './PaymentDuration';

export class PaymentDetails {
    private student: Student;
    private dormitoryNumber: number;
    private roomNumber: string;
    private duration: PaymentDuration;
    private price: Number;

    constructor(student: Student, dormitoryNumber: number, room: string, duration: PaymentDuration,
                price: Number) {
        this.student = student;
        this.dormitoryNumber = dormitoryNumber;
        this.roomNumber = room;
        this.duration = duration;
        this.price = price;
    }

    public static emptyData() {
        return new PaymentDetails(Student.emptyData(), null, null, null, null);
    }

    public getStudent(): Student {
        return this.student;
    }

    public setStudent(value: Student) {
        this.student = value;
    }

    public getDormitoryNumber(): number {
        return this.dormitoryNumber;
    }

    public setDormitoryNumber(value: number) {
        this.dormitoryNumber = value;
    }

    public getRoomNumber(): string {
        return this.roomNumber;
    }

    public setRoomNumber(value: string) {
        this.roomNumber = value;
    }

    public getDuration(): PaymentDuration {
        return this.duration;
    }

    public setDuration(value: PaymentDuration) {
        this.duration = value;
    }

    public getPrice(): Number {
        return this.price;
    }

    public setPrice(value: Number) {
        this.price = value;
    }
}