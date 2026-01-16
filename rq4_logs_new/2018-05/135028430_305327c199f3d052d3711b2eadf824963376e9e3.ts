import { Comparable } from "./Comparable";

export abstract class AbstractBasicComparator<T> implements Comparable<T> {
    protected readonly element: T;
    public constructor(element: T) {
        this.element = element;
    }

    public lessThan(toCompare: Comparable<T>): boolean {
        return this.getValue() < toCompare.getValue();
    }
    public greaterThan(toCompare: Comparable<T>): boolean {
        return this.getValue() > toCompare.getValue();
    }
    public equals(toCompare: Comparable<T>): boolean {
        return this.getValue() === toCompare.getValue();
    }
    public abstract getValue(): T;

}