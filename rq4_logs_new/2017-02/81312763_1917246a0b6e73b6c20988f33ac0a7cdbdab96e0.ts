import { Iterable } from './iterable'
import { expect } from 'chai'
import * as sinon from 'sinon'

describe('Iterable decorator', () => {
  it('should make the class iterable', () => {
    class Cart extends Iterable<string> {
      private articles: string[]

      public constructor(...articles) {
        super()
        this.articles = articles
      }

      public valid(key): boolean {
        return key < this.articles.length
      }

      public current(key): string {
        return this.articles[key]
      }
    }

    const callback = sinon.spy()

    const cart = new Cart('flour', 'eggs', 'milk')
    for (const article of cart) {
      callback(article)
    }

    expect(callback.callCount).to.equal(3)
    expect(callback.calledWith('flour')).to.be.true
    expect(callback.calledWith('eggs')).to.be.true
    expect(callback.calledWith('milk')).to.be.true
  })
})