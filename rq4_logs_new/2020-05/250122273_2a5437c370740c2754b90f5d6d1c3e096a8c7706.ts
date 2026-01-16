export function AutoUnsubscribe(blacklist: string[] = []): (c: any) => void {
  return function(constructor: any): void {
    const original = constructor.prototype.ngOnDestroy;

    constructor.prototype.ngOnDestroy = function(...args: any[]): void {
      for (const prop in this) {
        if (Object.prototype.hasOwnProperty.call(this, prop)) {
          const property = this[prop];

          if (!blacklist.includes(prop)) {
            if (property && typeof property.unsubscribe === 'function') {
              property.unsubscribe();
            }
          }
        }
      }

      if (original && typeof original === 'function') {
        original.apply(this, ...args);
      }
    };
  };
}