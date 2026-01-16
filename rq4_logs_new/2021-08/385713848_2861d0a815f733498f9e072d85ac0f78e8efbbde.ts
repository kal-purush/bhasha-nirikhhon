import { HttpContextContract } from '@ioc:Adonis/Core/HttpContext'

import Order from 'App/Models/Order'
import Product from 'App/Models/Product'
import { DateTime } from 'luxon'

export default class CloseOrdersController {
  public calculateTotalPrice(products: Product[]) {
    if (!products.length) return 0

    return products
      .map((product) => product.toJSON())
      .map((product) => +product.price * product.quantity)
      .reduce((pv, cp) => pv + cp)
  }

  public async update({ request, logger, response }: HttpContextContract) {
    const id = request.param('id')

    try {
      const order = await Order.query().where({ uuid: id }).preload('products').firstOrFail()

      const totalPrice = this.calculateTotalPrice(order.products)

      await order.merge({ closed: true, closedAt: DateTime.now(), totalPrice }).save()

      return {
        order: order.toJSON(),
      }
    } catch (error) {
      logger.error(error)

      return response.notFound({
        errors: [{ message: error.message }],
      })
    }
  }
}