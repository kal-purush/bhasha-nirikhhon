export const LoginRequired = (target, propertyKey: string, descriptor: PropertyDescriptor) => {
  target = propertyKey ? target : target.prototype

  if (!target._routes) {
    throw new ReferenceError('no routes found')
  }

  const routes = target._routes
  let handler = descriptor.value

  const index = routes.findIndex(route => {
    const routeHandler = route.handler
    return Array.isArray(routeHandler) ? routeHandler.includes(handler) : routeHandler === handler
  })

  const oldHandler = routes[index].handler

  handler = Array.isArray(oldHandler) ? oldHandler : [oldHandler]

  routes[index].handler = [
    (ctx, next) => {
      if (!ctx.session.user) {
        return ctx.redirect('/user/login')
      }

      next()
    },
    ...handler
  ]
}