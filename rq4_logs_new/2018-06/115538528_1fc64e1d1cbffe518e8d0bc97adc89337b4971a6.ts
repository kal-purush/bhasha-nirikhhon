
interface AsyncHasIterator<T>  extends AsyncIterator<T> {
    has(): Promise<boolean>;
}

interface AsyncHasIterable<T> {
    [Symbol.asyncIterator](): AsyncHasIterator<T>;
}

interface AsyncHasIterableIterator<T> extends AsyncHasIterator<T> {
    [Symbol.asyncIterator](): AsyncHasIterableIterator<T>;
}

interface HasIterator<T> extends Iterator<T>{
    has(): boolean
}

interface HasIterable<T> {
    [Symbol.iterator](): Iterator<T>;
}

interface HasIterableIterator<T> extends HasIterator<T> {
    [Symbol.iterator](): HasIterableIterator<T>;
}