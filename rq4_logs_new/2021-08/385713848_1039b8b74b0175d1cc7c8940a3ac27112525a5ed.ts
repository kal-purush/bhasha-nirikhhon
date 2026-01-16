import test from 'japa'

import Database from '@ioc:Adonis/Lucid/Database'
import { OrderFactory, ProductFactory } from 'Database/factories'
import { apiV1 } from '../../../utils'

test.group('ApiOrderProductsController', (group) => {
  group.beforeEach(async () => {
    await Database.beginGlobalTransaction()
  })

  group.afterEach(async () => {
    await Database.rollbackGlobalTransaction()
  })

  test('should POST in /api/v1/orders/:order_id/products creates a new OrderProduct', async (assert) => {
    const order = await OrderFactory.create()
    const product = await ProductFactory.create()

    const response = await apiV1
      .post(`/orders/${order.uuid}/products`)
      .set('Accept', 'application/json')
      .send({
        product: {
          id: product.id,
          quantity: 12,
        },
      })
      .expect(200)

    assert.exists(response.body.order)
    assert.exists(response.body.order.products)
    assert.lengthOf(response.body.order.products, 1)
  })

  test('should POST in /api/v1/orders/:order_id/products returns an error id product already exists', async (assert) => {
    const order = await OrderFactory.with('products', 1).create()

    await order.load('products')
    const [product] = order.products

    const response = await apiV1
      .post(`/orders/${order.uuid}/products`)
      .set('Accept', 'application/json')
      .send({
        product: {
          id: product.id,
          quantity: 12,
        },
      })
      .expect(409)

    assert.exists(response.body.errors)
  })

  test('should PUT in /api/v1/orders/:order_id/products/:id updates OrderProduct quantity', async (assert) => {
    const order = await OrderFactory.with('products', 1).create()

    await order.load('products')
    const [product] = order.products

    const response = await apiV1
      .put(`/orders/${order.uuid}/products/${product.id}`)
      .set('Accept', 'application/json')
      .send({
        quantity: 16,
      })
      .expect(200)
    assert.exists(response.body.order)

    await order.load('products')
    assert.lengthOf(order.products, 1)

    const [productUpdated] = order.products
    assert.equal(productUpdated.toJSON().quantity, 16)
  })

  test.only('should DELETE in /api/v1/orders/:order_id/products/:id deletes OrderProduct quantity', async (assert) => {
    const order = await OrderFactory.with('products', 1).create()

    await order.load('products')
    const [product] = order.products

    const response = await apiV1
      .delete(`/orders/${order.uuid}/products/${product.id}`)
      .set('Accept', 'application/json')
      .expect(200)

    assert.exists(response.body.order)

    await order.load('products')
    assert.lengthOf(order.products, 0)
  })

  test('should POST in /api/v1/orders/:order_id/products returns 404 if order doesnt exist', async () => {
    await apiV1
      .post(`/orders/94nvt93ycn32c42x9m38c4943v5/products`)
      .set('Accept', 'application/json')
      .send({
        product: {
          id: 1,
          quantity: 1,
        },
      })
      .expect(404)
  })
})