export function unwritableAndUnconfigurableObj() {
  return Object.defineProperty({}, "a", {
    value: 1,
    writable: false,
    enumerable: true,
    configurable: false,
  });
}

export function writableAndUnconfigurableObj() {
  return Object.defineProperty({}, "b", {
    value: 2,
    writable: true,
    enumerable: true,
    configurable: false,
  });
}

export function nestedUnwritableObj() {
  const obj = { c: { d: { e: 3 } } };

  const deepFreeze = (obj) => {
    Object.keys(obj).forEach((key) => {
      if (typeof obj[key] === "object" && obj[key] !== null) {
        deepFreeze(obj[key]);
      }
    });
    /**
     * オブジェクトを拡張不可にし、プロパティを再定義不可にするだけでなく、オブジェクトの独自データプロパティを読み出し専用にする
     */
    return Object.freeze(obj);
  };

  return deepFreeze(obj);
}