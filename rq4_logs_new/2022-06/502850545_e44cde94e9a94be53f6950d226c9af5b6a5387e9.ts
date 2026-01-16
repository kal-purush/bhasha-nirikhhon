import type { HttpContextContract } from '@ioc:Adonis/Core/HttpContext'
import Idioma from 'App/Models/Idioma'
import CreateIdiomaValidator from 'App/Validators/CreateIdiomaValidator'

export default class HabilidadesController {
  public async index({ response }: HttpContextContract) {
    const idiomas = await Idioma.query()

    response.ok({ data: idiomas })
  }

  public async show({ params: { id }, response }: HttpContextContract) {
    const idioma = await Idioma.findOrFail(id)

    return response.ok({ data: idioma })
  }

  public async store({ request, response }: HttpContextContract) {
    const validatedData = await request.validate(CreateIdiomaValidator)

    const idioma = await Idioma.create(validatedData)

    return response.created({ data: idioma })
  }

  public async update({ request, response, params: { id } }: HttpContextContract) {
    const idioma = await Idioma.findOrFail(id)

    const validatedData = await request.validate(CreateIdiomaValidator)
    idioma.merge(validatedData)
    await idioma.save()

    return response.created({ data: idioma })
  }

  public async destroy({ params: { id }, response }: HttpContextContract) {
    const idioma = await Idioma.findOrFail(id)

    await idioma.delete()

    return response.noContent()
  }
}