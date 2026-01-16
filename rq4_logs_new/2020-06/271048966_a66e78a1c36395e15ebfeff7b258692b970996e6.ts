import fetch from 'node-fetch'
import { FlavourInterface } from '../types'
const cheerioModule = require('cheerio')

export default async function getFlavorah() {
  const res = await fetch('https://store.flavorah.com/15ml-bottles')
  const $ = cheerioModule.load(await res.text())

  const flavors: FlavourInterface[] = []

  $('.product-info .name a').each(function () {
    const flavor = $(this)
    flavors.push({
      name: flavor
        .text()
        .trim()
        .replace(/(\r\n|\n|\r)/gm, '')
        .split(' Flavoring | ')[0],
      manufacturer: 'FLV',
    })
  })
  flavors.shift()
  return flavors
}