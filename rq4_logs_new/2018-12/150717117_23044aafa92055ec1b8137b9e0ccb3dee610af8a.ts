import { shield, and, rule } from 'graphql-shield'

const isAuthenticated = rule()((_, args, { user }) => user !== null)

const isAdmin = rule()(async (_, args, ctx) => {
  const { id } = ctx.user
  const scope = await ctx.prisma.user({ id }).role()
  return scope === 'ADMIN'
})

export const permissions = shield({
  Query: {
    me: isAuthenticated,
    user: and(isAuthenticated, isAdmin),
    users: and(isAuthenticated, isAdmin)
  },
  Mutation: {
    deleteMe: isAuthenticated,
    deleteManyUsers: and(isAuthenticated, isAdmin)
  }
})