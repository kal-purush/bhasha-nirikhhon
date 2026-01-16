interface Array<T> {
    add(item: T): void
	addRange(items: T[]): void
    clear(): void
    contains(item: T): boolean
    exists(match: (item: T) => boolean): boolean
    where(predicate: (item: T) => boolean): T[]

}