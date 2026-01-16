import * as http from 'http'
import * as path from 'path'
import * as fs from 'fs'
​
export interface MyHttpResponse {
  json: (item: any) => void
  send: (content: string) => void
}
​
export interface MyHttpRequest {
  params: (item: any) => void
}
​
class Express {

  [x: string]: any
​
  private server: any
  private routes: any = {}
​
  private readonly WWW_DIRECTORY = 'www'
  private readonly TEMPLATE_PAGE_DIRECTORY = 'pages'
  private readonly TEMPLATE_EXTENSION = '.html.mustache'
​
  constructor() {
    this._initialize()
  }

  listen(port: number, callback: () => void): void {
    this.server.listen(port, callback)
  }
​
  render(
    fileName: string,
    values: any,
    callback: (error: Error | null, html: string | null) => void
  ) {
    const pathName = path.join(
      process.cwd(),
      this.WWW_DIRECTORY,
      this.TEMPLATE_PAGE_DIRECTORY,
      `${fileName}${this.TEMPLATE_EXTENSION}`
    )
​
    if (!fs.existsSync(pathName)) {
      callback(new Error(`404 Page ${fileName} doesn't exist`), null)
      return
    }
​
    const content = fs.readFileSync(pathName, 'utf-8')
​
    const processContent = content.replace(
      /{{(\w+)( ?[|] ?)?(\w+)?}}/gi,
      (item: string, ...args: any[]): string => {
        const [key, pipe, transform] = args
        const v = values[key]
​
        if (!v) {
          return 'undefined'
        }

        if (pipe && transform) {
          const func = this[`_${transform}`]
          if (func) {
            return func(v)
          }
        } else {
          return v
        }
      }
    )
    
    callback(null, processContent)
  }
​
  /**
   * PRIVATE
   */
  private _initialize() {
    for (const verb of ['GET', 'POST', 'PUT', 'DELETE']) {
      this.routes[verb] = []
      this[verb.toLowerCase()] = (url: string, callback: any) => {
        this.routes[verb].push({ url, callback })
      }
    }
​
    this.server = http.createServer(
      (req: http.IncomingMessage, res: http.ServerResponse): void => {
        const { method, url } = req
​
        const response: MyHttpResponse = this._overrideReponse(res)
​
        const route = this.routes[method].find(item => item.url === url)
        if (!route) {
          res.statusCode = 404
          res.end()
          return
        }
​
        route.callback(req, response)
      }
    )
  }
​
  private _overrideReponse(res: http.ServerResponse): MyHttpResponse {
    const json = (item: any): void => {
      res.write(JSON.stringify(item))
      res.end()
    }
​
    const send = (content: string): void => {
      res.write(content)
      res.end()
    }
​
    return { ...res, json, send }
  }
​
  private _upper(str: string): string {
    return str.toUpperCase()
  }
​
  private _lower(str: string): string {
    return str.toLowerCase()
  }
}
​
export default () => new Express()