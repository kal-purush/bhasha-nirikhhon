import { assignDecorator, initDecorator } from '../util/init-decoration'
export let USE = (...middlewares) => (a1, a2?, a3?) => {

  // flatten the array for random array middleware usage
  middlewares = middlewares.reduce((pr, ne) => pr.concat(ne), [])

  switch (typeof a2) {
    case 'string':
      initDecorator(a1)
      assignDecorator(a1, {route: {[a2]: {middlewares}}})
      return
    default:
      initDecorator(a1)
      assignDecorator(a1, {option: {middlewares}})
      return
  }


}