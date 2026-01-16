import { Injectable } from '@angular/core';

/**
 * Class that just store data about instance and default configuration.
 * No logic involved.
 */
@Injectable({
  providedIn: 'root'
})
export class StoreService {

  private instanceConfig;
  private defaultConfig;

  constructor() { }

  setInstanceConfig(instanceConfig: any) {
    this.instanceConfig = instanceConfig;
  }

  setDefaultConfig(defaultConfig: any) {
    this.defaultConfig = defaultConfig;
  }

  /**
   * Get key from json configuration. If key is not present in instance
   * configuration method returns value from default configuration.
   * @param key
   */
  get(key: any) {
    const value = this.instanceConfig[key];
    if (value) {
      return value;
    } else {
      return this.defaultConfig[key];
    }
  }
}