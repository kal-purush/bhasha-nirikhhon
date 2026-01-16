import { HttpContextContract } from '@ioc:Adonis/Core/HttpContext'
import { schema, rules } from '@ioc:Adonis/Core/Validator'
import { v4 } from 'uuid'

import Catalog from 'App/Models/Catalog'
import Customer from 'App/Models/Customer'
import Order from 'App/Models/Order'

export default class OrdersController {
  public async store({ request }: HttpContextContract) {
    const validationSchema = schema.create({
      name: schema.string(),
      email: schema.string({}, [rules.email()]),
      phone: schema.string(),
    })

    const catalogId = await request.param('catalog_id')

    const { phone, email, name } = await request.validate({
      schema: validationSchema,
    })

    try {
      const catalog = await Catalog.findByOrFail('uuid', catalogId)
      const customer = await Customer.firstOrCreate({ phone }, { name, phone, email })

      const order = await Order.create({
        customerId: customer.id,
        userId: catalog.userId,
        uuid: v4(),
      })

      return {
        order: order.toJSON(),
        customer: customer.toJSON(),
      }
    } catch (error) {
      return {
        errors: [{ message: error.message }],
      }
    }
  }
}