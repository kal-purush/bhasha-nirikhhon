import BaseSeeder from '@ioc:Adonis/Lucid/Seeder'
import Category from 'App/Models/Category'
import File from 'App/Models/File'
import Product from 'App/Models/Product'

export default class ProductSeeder extends BaseSeeder {
  public async run() {
    const [relogio, chapeu] = await Category.createMany([
      {
        name: 'Relogio',
      },
      {
        name: 'Chapeu',
      },
    ])

    const productsPayload = [
      {
        name: 'Champion',
        categoryId: relogio.id,
        price: 150,
        codigo: '92837453',
      },
      {
        name: 'Rolex Submariner',
        categoryId: relogio.id,
        price: 250,
        codigo: '64738945',
      },
      {
        name: 'Apple wactch',
        categoryId: relogio.id,
        price: 800,
        codigo: '64739498',
      },
      {
        name: 'Panamá',
        categoryId: chapeu.id,
        price: 25,
        codigo: '02394586',
      },
      {
        name: 'Marcato',
        categoryId: chapeu.id,
        price: 50,
        codigo: '38985564',
      },
      {
        name: 'Café',
        categoryId: chapeu.id,
        price: 75,
        codigo: '90387545',
      },
    ]

    const products = await Product.createMany(productsPayload)

    const filesPayload = [
      {
        publicId: 'solasol_catalog_dev/p24oyfsuluda9ixy4ezo',
        secureUrl:
          'https://res.cloudinary.com/dalton10/image/upload/v1628827179/solasol_catalog_dev/p24oyfsuluda9ixy4ezo.jpg',
        uploaded: true,
        productId: products[0].id,
      },
      {
        publicId: 'solasol_catalog_dev/vyxnwtkpsyxhreqtg7ms',
        secureUrl:
          'https://res.cloudinary.com/dalton10/image/upload/v1628827217/solasol_catalog_dev/vyxnwtkpsyxhreqtg7ms.jpg',
        uploaded: true,
        productId: products[1].id,
      },
      {
        publicId: 'solasol_catalog_dev/ydioha0ko471jx7w6kw3',
        secureUrl:
          'https://res.cloudinary.com/dalton10/image/upload/v1628827257/solasol_catalog_dev/ydioha0ko471jx7w6kw3.jpg',
        uploaded: true,
        productId: products[2].id,
      },

      {
        publicId: 'solasol_catalog_dev/q5z11816uihf0logm7li',
        secureUrl:
          'https://res.cloudinary.com/dalton10/image/upload/v1628827398/solasol_catalog_dev/q5z11816uihf0logm7li.jpg',
        uploaded: true,
        productId: products[3].id,
      },
      {
        publicId: 'solasol_catalog_dev/qhxljigrm9xndgc68iv7',
        secureUrl:
          'https://res.cloudinary.com/dalton10/image/upload/v1628827430/solasol_catalog_dev/qhxljigrm9xndgc68iv7.jpg',
        uploaded: true,
        productId: products[4].id,
      },
      {
        publicId: 'solasol_catalog_dev/lmrnhgmnbly6nyyozloo',
        secureUrl:
          'https://res.cloudinary.com/dalton10/image/upload/v1628827473/solasol_catalog_dev/lmrnhgmnbly6nyyozloo.jpg',
        uploaded: true,
        productId: products[5].id,
      },
    ]

    await File.createMany(filesPayload)
  }
}