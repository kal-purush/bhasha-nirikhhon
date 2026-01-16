import accounting from "accounting";
import { Comparable } from "./Comparable";

export class MoneyComparable implements Comparable<number> {

    private readonly value: number;
    public constructor(money: number) {
        this.value = money;
    }
    public lessThan(toCompare: Comparable<number>): boolean {
        return this.getValue() < toCompare.getValue();
    }
    public greaterThan(toCompare: Comparable<number>): boolean {
        return this.getValue() > toCompare.getValue();
    }
    public equals(toCompare: Comparable<number>): boolean {
        return this.getValue() === toCompare.getValue();
    }
    public getValue(): number {
        return this.value;
    }

    public static parse(value: string): Comparable<number> {
        return new MoneyComparable(accounting.unformat(value));
    }
}