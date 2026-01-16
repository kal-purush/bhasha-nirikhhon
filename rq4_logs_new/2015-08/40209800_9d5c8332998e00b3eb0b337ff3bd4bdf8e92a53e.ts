import { parse, http } from './index'
import { expect } from 'chai'
import { get, createServer } from 'http'

describe('get headers', () => {
  it('should parse headers', () => {
    const headers = parse([
      'Content-Type: application/json',
      'Cookie: foo',
      'Cookie: bar',
      'Cookie: baz'
    ].join('\n'))

    expect(headers).to.deep.equal({
      'Content-Type': 'application/json',
      'Cookie': ['foo', 'bar', 'baz']
    })
  })

  it('should parse http', (done) => {
    const server = createServer((req, res) => {
      res.setHeader('Content-Type', 'application/json')
      res.setHeader('Cookie', <any> ['foo', 'bar', 'baz'])
      res.end('hello world')
    })

    server.on('listening', () => {
      get('http://localhost:' + server.address().port, (res) => {
        const headers = http(res)

        if (res.rawHeaders) {
          expect(headers['Content-Type']).to.equal('application/json')
          expect(headers['Cookie']).to.deep.equal(['foo', 'bar', 'baz'])
        } else {
          expect(headers['content-type']).to.equal('application/json')
          expect(headers['cookie']).to.deep.equal(['foo', 'bar', 'baz'])
        }

        server.close()

        return done()
      })
    })

    server.listen(0)
  })
})