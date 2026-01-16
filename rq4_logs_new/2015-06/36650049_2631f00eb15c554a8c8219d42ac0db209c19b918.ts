/// <reference path="../../typings/tsd.d.ts" />

import {OrderedModel} from "./ordered-model"
import {ModelStore} from "./model-store"

class MessageModel extends OrderedModel {
  constructor(public author: string,
              public content: string,
              public timestamp: Date) {
    super()
  }
}

class MessageStore extends ModelStore { }

export class MessageService {
  store: ModelStore = new MessageStore()

  add(author, content, timestamp) {
    this.store.add(new MessageModel(author, content, timestamp))
  }

  findAll() {
    return this.store.findAll();
  }

  forEach(callback) {
    this.store.forEach(callback)
  }
}