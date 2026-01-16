export = store;

declare var store: Store;

interface Store {
    /** Retrieve a value from browser persistent storage */
    (key: string): any;
    
    /** Store a value in browser persistent storage */
    (key: string, value: any): void;
}