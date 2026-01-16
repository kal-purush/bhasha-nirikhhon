//generator + promise with runner pattern
function* gen() {
  const promiseVal = yield new Promise((resolve) => resolve(42));
  console.log(promiseVal);
  const val = yield "hello";
  console.log(val);
  const promiseVal2 = yield new Promise((resolve) => resolve(13));
  console.log(promiseVal2);
}

function runner(genFn) {
  const iterator = genFn();
  return run();

  function run(arg) {
    const result = iterator.next(arg);
    if (result.done) return result.value;
    return Promise.resolve(result.value).then(run);
  }
}

runner(gen);

//async await
async function equivalent() {
  const promiseVal = await new Promise((resolve) => resolve(42));
  const val = await promiseVal + 1;
  return val;
}

equivalent().then(console.log);