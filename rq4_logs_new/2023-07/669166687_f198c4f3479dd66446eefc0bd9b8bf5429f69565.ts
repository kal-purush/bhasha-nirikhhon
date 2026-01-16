/*
https://leetcode.com/problems/generate-parentheses/description/
2. Generate Parentheses
Given n pairs of parentheses, write a function to generate all combinations of well-formed parentheses.

Example 1:
Input: n = 3
Output: ["((()))","(()())","(())()","()(())","()()()"]

Example 2:
Input: n = 1
Output: ["()"]

Constraints:
1 <= n <= 8
*/
const combine = (as: string[], bs: string[]): string[] => {
  return as.flatMap(a => bs.flatMap(b => [a + b, b + a]));
};

const cache = new Map<number, string[]>();
const memoize = (f: (x: number) => string[]) => (n: number) => {
  if (cache.has(n)) {
    return cache.get(n);
  }
  cache.set(n, f(n));
  return cache.get(n);
}

const generateParenthesis = memoize((n: number): string[] => {
  if (n === 0) { return []; }
  if (n === 1) {
    return ["()"];
  }

  let allParenthesis: string[] = [];

  allParenthesis.push(...generateParenthesis(n - 1).map(p => "(" + p + ")"));

  for (let i = 1; i < n; i++) {
    allParenthesis.push(...combine(generateParenthesis(i), generateParenthesis(n - i)));
  }

  return [...new Set(allParenthesis)];
});