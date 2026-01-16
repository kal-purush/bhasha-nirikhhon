import { init, regisHandler, setExposeKey, setIgnoreKey, setState, setVar } from '../core'

export function Init(proto: any, key: PropertyKey) {
  setVar(proto, key)

  regisHandler(proto, key, {
    async init(instance: any) {
      return instance[key]()
    },
  })
}

export function Unmount(proto: any, key: PropertyKey) {
  setVar(proto, key)

  regisHandler(proto, key, {
    async unmount(instance: any) {
      return instance[key]()
    },
  })
}

// bind value
// won't assign
export function Bind(value: any) {
  return (proto: any, k: PropertyKey) => {
    setVar(proto, k)
    setState(proto, k, {
      value,
    })
  }
}

export function Ignore(proto: any, key: PropertyKey) {
  setIgnoreKey(proto, key)
}

export function Clear(proto: any, key: PropertyKey) {
  init(proto)

  proto._namespace.__EXPOSE_VAR__.delete(key)
  proto._namespace.__IGNORE_VAR__.delete(key)
  proto._namespace.__STATE_VAR__.delete(key)
  proto._namespace.__STATE_HANDLER__.delete(key)
  proto._namespace.__STATE_NAMESPACE__.delete(key)
}

export function Expose(proto: any, key: PropertyKey) {
  setExposeKey(proto, key)
}

export function Empty(module: any) {
  init(module.prototype)
}

// export function Nested<M extends new (...args: any) => any>(module: M) {
//   return To(async (property) => {
//     const instance = plainToClass(module, property)

//     const err = await transformClass(instance)
//     if (err.length > 0)
//       throw new Error(err[0])

//     return instance
//   })
// }