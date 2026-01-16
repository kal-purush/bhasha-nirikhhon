import * as R from 'ramda';

// Array transformations using Ramda
export const filterPositiveNumbers = R.filter((n: number) => n > 0);

export const doubleAllNumbers = R.map(R.multiply(2));

export const sumOfArray = R.sum;

// Functional composition with Ramda
export const getPositiveNumbersSum = R.pipe(
  filterPositiveNumbers,
  sumOfArray
);

// Object manipulation with Ramda
export const pickUserData = R.pick(['id', 'name', 'email']);

export const renameKeys = R.curry((keysMap: Record<string, string>, obj: Record<string, unknown>) =>
  R.reduce(
    (acc, key) => {
      const newKey = keysMap[key] || key;
      return R.assoc(newKey, R.prop(key, obj), acc);
    },
    {},
    R.keys(obj)
  )
);

// Function with Ramda for data transformation
interface User {
  active: boolean;
  name: string;
  password?: string;
  token?: string;
  [key: string]: unknown;
}

export const processUserList = R.pipe(
  R.filter((user: User) => user.active === true),
  R.sortBy(R.prop('name') as (user: User) => string),
  R.map((user: User) => {
    // 型安全なomit操作
    const { password, token, ...rest } = user;
    return rest;
  })
);

// Functional conditionals
export const getDiscountRate = R.cond([
  [R.propSatisfies(R.lt(1000), 'totalSpent'), R.always(0.2)],   // 20% for > $1000
  [R.propSatisfies(R.lt(500), 'totalSpent'), R.always(0.1)],    // 10% for > $500
  [R.propSatisfies(R.lt(100), 'totalSpent'), R.always(0.05)],   // 5% for > $100
  [R.T, R.always(0)],                                          // 0% otherwise
]);