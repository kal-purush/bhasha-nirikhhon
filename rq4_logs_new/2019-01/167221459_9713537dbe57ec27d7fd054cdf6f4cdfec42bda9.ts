const isObject = <TypedObj extends object>(value: any): value is TypedObj =>
  typeof value === 'object' && typeof value !== 'function' && value != undefined;

const isContainer = <T>(children: any, expectedProp: string): children is T => {
  return isObject(children) && expectedProp in children;
}


export { isObject, isContainer }