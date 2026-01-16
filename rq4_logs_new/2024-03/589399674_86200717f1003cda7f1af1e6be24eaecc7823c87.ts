function ensureExhaustive(_x: never): never {
  throw new Error("Reached a branch with non-exhaustive checks");
}

export { ensureExhaustive };