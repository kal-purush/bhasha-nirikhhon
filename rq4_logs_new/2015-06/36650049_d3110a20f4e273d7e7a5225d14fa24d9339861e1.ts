import {OrderedModel} from "./ordered-model"
import {ListWrapper} from "../../node_modules/angular2/src/facade/collection"

export class ModelStore {
  items: List<OrderedModel> = []

  add(record: OrderedModel) {
    ListWrapper.push(this.items, record)
  }

  remove(record: OrderedModel) {
    var i = this.items.indexOf(record)
    if (i > -1) {
      var spliced = ListWrapper.splice(this.items, i, 1)
      return spliced[0]
    }

    return null
  }

  removeMatching(pred: Function) {
    var records = ListWrapper.filter(this.items, pred)
    ListWrapper.removeAll(this.items, records)
    return records
  }
}