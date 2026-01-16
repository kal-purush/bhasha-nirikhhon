import { HttpContextContract } from '@ioc:Adonis/Core/HttpContext'
import Catalog from 'App/Models/Catalog'
import Category from 'App/Models/Category'

export default class CategoriesController {
  public async index({ request, response, logger }: HttpContextContract) {
    const catalogUuid = request.param('catalog_id')

    try {
      const catalog = await Catalog.findByOrFail('uuid', catalogUuid)

      if (catalog.expired) {
        return response.gone()
      }

      const categories = await Category.all()

      return {
        catalog: catalog.toJSON(),
        categories: categories.map((i) => i.toJSON()),
      }
    } catch (error) {
      logger.error(error)

      return response.notFound()
    }
  }
}