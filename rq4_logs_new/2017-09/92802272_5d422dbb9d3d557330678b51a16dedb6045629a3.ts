import { ResourceTemplate, ShopTemplate, Capitalist } from './templates'
import { IGameObject } from 'quarkit-core'
import { ResourceSlot, Production, IProduction } from 'quarkit-modules'
import { Colors } from './lib/Colors'
import * as Humanize from 'humanize-plus'

const moment = require('moment')
moment.updateLocale('en', {
    relativeTime : {
        future: "in %s",
        past:   "%s ago",
        s:  "%d seconds",
        m:  "a minute",
        mm: "%d minutes",
        h:  "an hour",
        hh: "%d hours",
        d:  "a day",
        dd: "%d days",
        M:  "a month",
        MM: "%d months",
        y:  "a year",
        yy: "%d years"
    }
});
export class GameClient {
  private _player:Capitalist
  constructor (player:Capitalist) {
    this._player = player
  }

  buyShop(shop:ShopTemplate) {
    shop.purchaseFor(this._player)
    console.log(`Player ${Colors.slug(this._player.Slug)} buy Shop ${Colors.slug(shop.Slug)}`)
  }

  getInnerBag() : ResourceSlot[] {
    this._player.PossessionsObjects.map((go:any) => {
      if (go instanceof Production) {
        go.applyProduction(this._player)
      }
    })
    return this._player.InnerBag
  }

  printPlayer() {
    console.log(Colors.lite("=============================="))
    const resBag = this.getInnerBag().map(e => `${e.Resource.Slug} = ${Humanize.intComma(e.Amount)}`,1).join(', ')
    console.log(`Player ${Colors.slug(this._player.Slug)} (${resBag})`)
    this._player.PossessionsObjects.map(this.printAct)
    console.log(Colors.lite("=============================="))
  }

  printAct(go:IProduction&IGameObject) {
    console.log(`  Shop ${Colors.slug(go.Slug)} (next ${Humanize.intComma(go.ProductionSlots[0].Amount)} ${printSpan(go.LastProductionTime + go.ProductionBaseTime)})`)
  }
}


function printSpan(d) {
  return moment(d).fromNow()
}