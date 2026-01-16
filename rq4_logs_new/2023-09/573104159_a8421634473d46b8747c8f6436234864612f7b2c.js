//Promise.any takes an array of promises and returns a single promise that fulfills with the value of the fulfilled promise. If no promises fulfill then it rejects with the aggreagted result of the failed promises.

const myPromiseAny = function (promisesArr) {
  let promiseError = new Array(promisesArr.length);

  let counter = 0;
};