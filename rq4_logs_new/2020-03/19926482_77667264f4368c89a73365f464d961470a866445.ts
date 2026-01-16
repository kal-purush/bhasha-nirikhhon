import Trie from './Trie'
import cityNames from './testData/cityNames'

describe('Trie', () => {
  it('finds all words beginning with a particular prefix', () => {
    const trie = new Trie()
    cityNames.forEach((name) => trie.add(name))

    const completions = trie.getCompletions('san')
    const expectedNames = [
      'san antonio',
      'san angelo',
      'san diego',
      'san jose',
      'san jacinto',
      'san francisco',
      'san bernardino',
      'san buenaventura',
      'san bruno',
      'san mateo',
      'san marcos',
      'san leandro',
      'san luis obispo',
      'san ramon',
      'san rafael',
      'san clemente',
      'san gabriel',
      'santa ana',
      'santa clarita',
      'santa clara',
      'santa cruz',
      'santa rosa',
      'santa maria',
      'santa monica',
      'santa barbara',
      'santa fe',
      'santee',
      'sandy',
      'sandy springs',
      'sanford'
    ].sort()
    expect(completions.sort()).toEqual(expectedNames)
  })

  it('handles whole words', () => {
    const trie = new Trie()
    cityNames.slice(0, 30).forEach((name) => trie.add(name))
    const completions = trie.getCompletions('seattle')

    expect(completions.length).toBe(1)
    expect(completions[0]).toBe('seattle')
  });

  it('handles no match', () => {
    const trie = new Trie()
    cityNames.slice(0, 30).forEach((name) => trie.add(name))
    const completions = trie.getCompletions('no match')

    expect(completions.length).toBe(0)
  });

  it('handles words that are a subset of another string', () => {
    const trie = new Trie()
    cityNames.slice(0, 800).forEach((name) => trie.add(name))
    const completions = trie.getCompletions('salin')

    expect(completions).toEqual(['salina', 'salinas'])
  });
})