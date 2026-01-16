/**
 * An object that simplifies saving and loading game data.
 */
class StorageDevice {
    private storageKey: string;
    private loadedData: any;
    private bindings: {key: string, cmp: Component}[] = [];

    constructor(storageKey: string) {
        this.storageKey = storageKey;

        // Immediately load data from previous session (if such data exists)
        this.tryLoadSession();
    }

    private tryLoadSession(): void {
        let data: any = localStorage.getItem(this.storageKey);

        if (!data) {
            // Previous save doesn't exist
            return;
        }

        try {
            data = JSON.parse(data);
        } catch (ex) {
            // Data is corrupt?
            return;
        }

        console.log("Data loaded: ", data);
        this.loadedData = data;
    }

    public bind(key: string, cmp: Component): void {
        // Load data for this component immediately, it there is any
        const loadedData = this.loadedData;
        if (loadedData) {
            if (loadedData.hasOwnProperty(key)) {
                cmp.val = loadedData[key];
            }
        }

        // Ensure that both keys and components are unique.
        // A duplicate key or component will both result in
        // save data overwriting.
        const bindings = this.bindings;
        for (let i = 0; i < bindings.length; i++) {
            const b = bindings[i];
            if (b.key === key) {
                // Key collision
                throw new Error("Binding keys for components must be unique.");
            }
            if (b.cmp === cmp) {
                // Component collision
                throw new Error("The same component cannot be bound twice to the same storage device.");
            }
        }

        this.bindings.push({key, cmp});
    }

    public save(): void {
        // Populate an object with the required save data
        const data = {};

        const bindings = this.bindings;
        for (let i = 0; i < bindings.length; i++) {
            const b = bindings[i];
            data[b.key] = b.cmp.val;
        }

        localStorage.setItem(this.storageKey, JSON.stringify(data));
    }
}