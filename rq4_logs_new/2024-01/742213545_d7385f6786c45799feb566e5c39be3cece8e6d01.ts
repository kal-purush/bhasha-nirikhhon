import Elysia from 'elysia'

import { db } from '~/db/connection'

import { authentication } from '../authentication'
import { NotFoundError } from '../errors/not-found-error'
import { restaurantModel } from '../models/restaurant-model'

export const getManagedRestaurant = new Elysia().use(authentication).get(
  '/managed-restaurant',
  async ({ getManagedRestaurantId }) => {
    const restaurantId = await getManagedRestaurantId()

    const restaurant = await db.query.restaurants.findFirst({
      where(fields, { eq }) {
        return eq(fields.id, restaurantId)
      },
    })

    if (!restaurant) {
      throw new NotFoundError('Restaurant not found.')
    }

    return restaurant
  },
  {
    response: {
      200: restaurantModel,
    },
  }
)