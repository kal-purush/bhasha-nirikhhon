interface Array<T> {
    /** Iterator */
    [Symbol.iterator](): IterableIterator<T>;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, T]>;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<T>;
}

interface Map<K, V> {
    entries(): IterableIterator<[K, V]>;
    keys(): IterableIterator<K>;
    values(): IterableIterator<V>;
    [Symbol.iterator](): IterableIterator<[K, V]>;
}

interface Set<T> {
    entries(): IterableIterator<[T, T]>;
    keys(): IterableIterator<T>;
    values(): IterableIterator<T>;
    [Symbol.iterator](): IterableIterator<T>;
}

interface ObjectConstructor {
    values(object: any): any[];
    entries(object: any): [string, any][];
    getOwnPropertyDescriptors(object: any): PropertyDescriptorMap;
}