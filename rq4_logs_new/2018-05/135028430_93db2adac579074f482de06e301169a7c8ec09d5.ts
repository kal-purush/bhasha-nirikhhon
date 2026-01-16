import { Comparable } from "./Comparable";

export class StringComparable implements Comparable<string> {

    private element: string;
    public constructor(element: string) {
        this.element = element;
    }
    public lessThan(toCompare: Comparable<string>): boolean {
        return this.getValue() < toCompare.getValue();
    }
    public greaterThan(toCompare: Comparable<string>): boolean {
        return this.getValue() > toCompare.getValue();
    }
    public equals(toCompare: Comparable<string>): boolean {
        return this.getValue() === toCompare.getValue();
    }
    public getValue(): string {
        return this.element.toLocaleLowerCase();
    }
}