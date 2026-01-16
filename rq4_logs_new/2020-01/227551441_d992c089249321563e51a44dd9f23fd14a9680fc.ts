import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
/**
 * Extends this class to make any class a temporary-cache-like class.
 *
 * Especially useful for Angular services, so that you can store any value in it from _e.g._ a component,
 * and retrieve this stored value from another component.
 *
 * This is a generic class to which you can specify the type of data that this cache should accept.
 */
export class CacheService<T> {

  private cache: T;

  /**
   * Set the cache value
   * @param cache The value to cache
   */
  setCache(cache: T) {
    this.cache = cache;
  }

  /**
   * Get the previously cached value and empty the current cache
   */
  getCache(): T {
    const cache = this.cache;
    this.cache = null;
    return cache;
  }

  /**
   * Checks if there's a value in the cache
   */
  hasCache(): boolean {
    return Boolean(this.cache);
  }

}