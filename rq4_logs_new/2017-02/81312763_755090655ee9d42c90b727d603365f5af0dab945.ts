import { Iterable } from 'ts-iterable'

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

const cart = new Cart('flour', 'eggs', 'milk')
for (const article of cart) {
  console.log(article)
}

// Displays:
// flour
// eggs
// milk