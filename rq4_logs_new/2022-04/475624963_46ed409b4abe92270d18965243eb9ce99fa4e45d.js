const { readFileSync } = require('fs')

const makeQueryExample = fnName => `viddy.${fnName}('element with text')
viddy.${fnName}(/regex/i, { near: 'element with text' })
viddy.${fnName}({ pattern: /regex/i, pickParent: 'p' })
viddy.${fnName}({ selector: 'p', leftOf: 'text' })`

const makeQueryValueExample =
  fnName => `viddy.${fnName}('exact value', 'element with text')
viddy.${fnName}(/okay/i, { selector: 'select', near: 'Choose:' })
viddy.${fnName}('on', { selector: 'input[type=checkbox]' })`

const jsdocExamples = [
  {
    match: /\bfor:/,
    description: 'Return element matching query',
    example: makeQueryExample('for')
  },
  {
    match: /\bforInput:/,
    description:
      'Return nearest <input>, <select>, or <textarea> matching query',
    example: makeQueryExample('forInput')
  },
  {
    match: /\bselectorOf:/,
    description: 'Return unique CSS selector matching query',
    example: makeQueryExample('selectorOf')
  },
  {
    match: /\bvalueOf:/,
    description:
      'Return value of nearest <input>, <select>, or <textarea> matching query',
    example: makeQueryExample('valueOf')
  },
  {
    match: /\bwaitFor:/,
    description:
      'Return Promise that awaits the query, returning matching elements',
    example: makeQueryExample('waitFor')
  },
  {
    match: /\bwaitForValue:/,
    description:
      'Return Promise that awaits the query, returning nearest matching <input>, <select>, or <textarea> elements',
    example: makeQueryValueExample('waitForValue')
  },
  {
    match: /\bwaitForIdle:/,
    description:
      'Return Promise that waits for entire DOM to stop updating, or a portion of it if a query is specified',
    example: `viddy.waitForIdle()\n` + makeQueryExample('waitForIdle')
  },
  {
    match: /\binnerText:/,
    description: 'Return innerText of the elements matching query',
    example: makeQueryExample('innerText')
  },
  {
    match: /\bmatchText:/,
    description:
      'Return innerText of the elements matching query. If RegExp is specified as the pattern, only the matching-portion of the innerText will be returned. If a capturing-group is specified within the RegExp, the full result-array will be returned',
    example: makeQueryExample('matchText')
  },
  {
    match: /\bhasContent:/,
    description: 'Return true if elements are found matching the query',
    example: makeQueryExample('hasContent')
  },
  {
    match: /export function viddyIn/,
    description: 'Inject viddy API into a Puppeteer page object',
    example: `const { viddyIn } = require('viddy/puppeteer')
// later...
const page = await browser.newPage()
const viddy = viddyIn(page)

await viddy.waitFor('text')`
  },
  {
    match: /export function viddyWellIn/,
    description: 'Inject viddyWell API into a Puppeteer page object',
    example: `const { viddyWellIn } = require('viddy/puppeteer')
// later...
const page = await browser.newPage()
const viddy = viddyWellIn(page)

await viddy.waitFor('text')`
  }
]

const makeJsdocComment = ({ description = '', example }) => `
  /**
    * ${description}
    * @example
${example
  .split('\n')
  .map(line => `    * ${line}`)
  .join('\n')}
    */
`

const typedefsWithJsdoc = readFileSync('./build-config/index.d.ts-base', 'utf8')
  .split('\n')
  .map(line => {
    const example = jsdocExamples.find(example => example.match.test(line))
    return example ? makeJsdocComment(example) + line : line
  })
  .join('\n')

console.log(typedefsWithJsdoc)