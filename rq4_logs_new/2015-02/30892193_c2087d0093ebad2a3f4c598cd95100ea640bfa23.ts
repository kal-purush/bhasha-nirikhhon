/**
 * @license
 * Typie - type safe type coercions for Javascript and Typescript
 * Copyright (c) 2015 Ken Sheedlo
 * License: MIT
 */

module Typie {
  'use strict';
  var toString:(x:any)=> string = Object.prototype.toString;

  function isNumber(x:any): boolean {
    return toString.call(x) === '[object Number]';
  }

  function isUndefined(x:any): boolean {
    return x === void 0;
  }

  export function toInt(x:any, base?:any): number {
    var base_: number = 10;
    if (!isUndefined(base)) {
      if (!isNumber(base)) {
        throw new Error('[Typie:nobase] Base must be a Number, but got ' + base);
      } else if (base < 2 || base > 36) {
        throw new Error('[Typie:baserange] Base must be between 2 and 36, but got ' + base);
      } else {
        base_ = base;
      }
    }
    return parseInt(x, base_);
  }

  export function toBoolean(x:any): boolean {
    return !!x;
  }

  export function toFloating(x:any): number {
    return +x;
  }
}