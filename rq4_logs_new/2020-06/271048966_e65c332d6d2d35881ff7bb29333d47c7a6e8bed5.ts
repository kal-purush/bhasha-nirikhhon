import fetch from 'node-fetch'
import { FlavourInterface } from '../types'
const cheerioModule = require('cheerio')

export default async function getFlavorWest() {
  const res = await fetch(
    'https://www.flavorwest.com/water-soluble-flavoring.html?product_list_limit=all'
  )
  const $ = cheerioModule.load(await res.text())

  const flavors: FlavourInterface[] = []

  $('.product-item-link').each(function () {
    const flavor = $(this)
    flavors.push({
      name: flavor
        .text()
        .trim()
        .replace(/(\r\n|\n|\r)/gm, ''),
      manufacturer: 'FW',
    })
  })
  flavors.shift()
  return flavors
}