import { Comparable } from "./Comparable";

export class NumberComparable implements Comparable<number> {

    private element: number;
    public constructor(element: number) {
        this.element = element;
    }
    public lessThan(toCompare: Comparable<any>): boolean {
        return this.getValue() < toCompare.getValue();
    }
    public greaterThan(toCompare: Comparable<any>): boolean {
        return this.getValue() > toCompare.getValue();
    }
    public equals(toCompare: Comparable<any>): boolean {
        return this.getValue() === toCompare.getValue();
    }
    public getValue(): number {
        return this.element;
    }
    public static parse(value: string): Comparable<number>  {
        const parsedInt = Number(value);
        if (isNaN(parsedInt)) {
            throw new Error("Invalid number");
        }
        return new NumberComparable(parsedInt);
    }
}