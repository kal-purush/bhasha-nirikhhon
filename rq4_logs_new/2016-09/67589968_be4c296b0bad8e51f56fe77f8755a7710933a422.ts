// Add Array.from() function to the ArrayConstructor interface, because TypeScript wouldn't let me use it even with ES6 for some reason
interface ArrayConstructor {
  from(arrayLike: any, mapFn?, thisArg?): Array<any>;
}