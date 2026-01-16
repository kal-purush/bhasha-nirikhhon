function recursiveProxy(value, handler) {
  if (typeof value !== 'object' || value === null) {
    return value
  }
  return new Proxy(value, handler)
}

function observable(value) {
  const handler = {
    get(target, key) {
      return target[key]
    },
    set(target, key, value) {
      console.log('notify')
      target[key] = value
      return true
    },
  }
  return new Proxy(
    {
      value: recursiveProxy(value, handler),
    },
    handler
  )
}

const x = observable([1, 2, 3])

x.value.push(4)

x