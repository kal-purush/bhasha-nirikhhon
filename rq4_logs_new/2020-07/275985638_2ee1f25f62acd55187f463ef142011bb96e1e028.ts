import { SessionRequestHandlerBase } from "~/src/server/gyst-server/express-server-endpoint-collection/endpoint-base/session"

export abstract class RemoveDataBase<T,U,V> extends SessionRequestHandlerBase {
  abstract handleAccessToken0():Promise<T>
  abstract handleServiceSetting1():Promise<U>
  abstract handleConnectedAccount2():Promise<V>

  async removeData() {
    const result0 = await this.handleAccessToken0()
    const result1 = await this.handleServiceSetting1()
    const result2 = await this.handleConnectedAccount2()
    return [result0, result1, result2]
  }
}