
// Using the `await` keyword to reslove an async function.
async function foo () {
  return await asyncFn()
}

// Resloves the async function without `await` keyword.
async function foo () {
  return asyncFn()
}

// With the `await` keyword we can catch errors inside `foo`
async function foo () {
  try {
    return await asyncFn()
  } catch (error) {
    // here we catch errors from asyncFn.
  }
}


// Without the `await` keyword the error is passed outside `.catch`
async function foo () {
  try {
    return asyncFn()
  } catch (error) {
    // errors from asyncFn do not land here.
  }
}

foo()
  .catch((error) => {
    // error from foo will land here.
  })