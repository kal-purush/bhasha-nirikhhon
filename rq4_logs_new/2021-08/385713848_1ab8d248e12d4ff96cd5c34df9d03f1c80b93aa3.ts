import { HttpContextContract } from '@ioc:Adonis/Core/HttpContext'

export default class OrdersController {
  public async index({ view }: HttpContextContract) {
    const orders = [
      {
        id: 1,
        codigo: 'GF43D',
        client: 'Yasmin Braga',
        total: 'R$122,00',
        openedDate: '12/12/12',
        closeDate: '-',
        status: 'opened',
      },

      {
        id: 2,
        codigo: 'GF43D',
        client: 'Rosa Teixeira',
        total: 'R$122,00',
        openedDate: '12/12/12',
        closeDate: '-',
        status: 'effectuated',
      },

      {
        id: 3,
        codigo: 'GF43D',
        client: 'Daniel Braga',
        total: 'R$122,00',
        openedDate: '12/12/12',
        closeDate: '-',
        status: 'confirmed',
      },

      {
        id: 4,
        codigo: 'GF43D',
        client: 'Rosa Braga',
        total: 'R$122,00',
        openedDate: '12/12/12',
        closeDate: '-',
        status: 'confirmed',
      },
    ]

    return view.render('orders/index', { orders })
  }

  public async create({}: HttpContextContract) {}

  public async store({}: HttpContextContract) {}

  public async show({}: HttpContextContract) {}

  public async edit({}: HttpContextContract) {}

  public async update({}: HttpContextContract) {}

  public async destroy({}: HttpContextContract) {}
}