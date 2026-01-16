import { subscriptionType } from '@nexus/schema'

export const Subscription = subscriptionType({
  definition(t) {
    t.field('latestOrder', {
      type: 'orders',
      subscribe(_root, _args, ctx) {
        return ctx.pubsub.asyncIterator('latestOrder')
      },
      resolve(payload) {
        return payload
      },

    })

    t.field('latestCheckout', {
      type: 'checkouts',
      subscribe(_root, _args, ctx) {
        return ctx.pubsub.asyncIterator('latestCheckout')
      },
      resolve(payload) {
        return payload
      },

    })
  }
})