import IRange from './IRange'
import INumericallyComparable from './INumericallyComparable'

export default class ClosedRange<T extends INumericallyComparable> implements IRange<T> {
    public static fromTo<T extends INumericallyComparable>(lower: T, upper: T): ClosedRange<T> {
        return new ClosedRange(lower, upper)
    }

    public readonly lower: T
    public readonly upper: T

    private constructor(lower: T, upper: T) {
        if (lower > upper) {
            throw new RangeError(`Range ${lower}...${upper} is invalid`)
        }
        this.lower = lower
        this.upper = upper
    }

    public containsValue(value: T) {
        return value >= this.lower && value <= this.upper
    }

    public containsRange(range: IRange<T>) {
        return this.containsValue(range.lower) && this.containsValue(range.upper)
    }

    public overlapsRange(range: IRange<T>) {
        return this.containsValue(range.lower) || this.containsValue(range.upper)
    }

    public union(otherRange: ClosedRange<T>) {
        if (!this.overlapsRange(otherRange)) {
            throw new Error(
                `Cannot form union of ${this.toString()} and ${otherRange.toString()}; ` +
                'they do not overlap'
            )
        }

        return ClosedRange.fromTo(
            this.lower < otherRange.lower ? this.lower : otherRange.lower,
            this.upper > otherRange.upper ? this.upper : otherRange.upper
        )
    }

    public equals(otherRange: ClosedRange<T>): boolean {
        return otherRange.lower === this.lower && otherRange.upper === this.upper
    }

    public toString() {
        return `${this.lower}...${this.upper}`
    }
}