declare class ConfigStore {
  constructor (packageName: string, defaults?: Object, options?: ConfigStore.Options)

  /**
   * Set an item.
   */
  set (key: string, value: any): void;
  /**
   * Set multiple items at once.
   */
  set (object: Object): void;
  /**
   * Get an item.
   */
  get (key: string): any;
  /**
   * Delete an item.
   */
  del (key: string): void;
  /**
   * Delete all items.
   */
  clear (): void;
  /**
   * Get the item count.
   */
  size: number;
  /**
   * Get all items as an object or replace the current config with an object.
   */
  all: Object;
  /**
   * Get the path to the config file. Can be used to show the user where the config file is located or even better open it for them.
   */
  path: string;
}

declare namespace ConfigStore {
  export interface Options {
    /**
     * Store the config at `$CONFIG/package-name/config.json` instead of the default `$CONFIG/configstore/package-name.json`. This is not recommended as you might end up conflicting with other tools, rendering the "without having to think" idea moot.
     */
    globalConfigPath: boolean;
  }
}

export = ConfigStore;