import { Schemas } from "../serialize/Schema.js";
import { Storage } from "./Storage.js";

/**
 * An instance of 'Settings' a set of configuration options for the game.
 */
export class SettingsOptions {

    // Default settings values
    private constructor(
        // width of viewable area in tiles
        public viewWidth: number = 12,
        // height of viewable area in tiles
        public viewHeight: number = 8,
        // game volume
        public volume: number = 0,
    ) {}

    static defaultOptions(): SettingsOptions {
        return new SettingsOptions();
    }

    static schema = Schemas.classOf({
        viewWidth: Schemas.aNumber,
        viewHeight: Schemas.aNumber,
        volume: Schemas.aNumber,
    }, ({ viewWidth, viewHeight, volume }) => {
        return new SettingsOptions(viewWidth, viewHeight, volume);
    });
}

export namespace Settings {
    // The key to use in local storage
    const lsKey = "settings" as const;

    /**
     * Load a stored set of options from local storage, or if there is none,
     * load the default set of options. Does not assign it as the current set of
     * options; to do this, use 'Settings.saveOptions'.
     */
    export function loadOptions(): SettingsOptions {
        const stored = Storage.loadItem(lsKey, SettingsOptions.schema);
        if (stored?.resultType === "success") {
            return stored.result;
        } else {
            return SettingsOptions.defaultOptions();
        }
    }

    export let currentOptions: SettingsOptions = loadOptions();

    /**
     * Saves a set of options to the store, and sets the current set of options
     * to the set provided.
     */
    export function saveOptions(options: SettingsOptions): void {
        currentOptions = options;
        Storage.saveItem(lsKey, options, SettingsOptions.schema);
    }

    /**
     * Replace all current and saved settings with the default settings.
     */
    export function restoreDefaults(): void {
        Settings.saveOptions(SettingsOptions.defaultOptions());
    }
}