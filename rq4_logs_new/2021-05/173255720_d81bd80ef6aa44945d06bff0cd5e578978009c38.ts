import { MixedObject } from '../types'
import { ApiService, CookieService } from '.'
import { isCrawler } from '../core/utils'

const csrfCookieName = 'ds-csrf'
const customerIdCookieName = 'ds-custid'

export class TokenService {
  static touchToken(clientId: string, csrfAfter: number): Promise<string> {
    return new Promise((resolve: any) => {
      const customerId = CookieService.get(customerIdCookieName)
      const csrftoken = CookieService.get(csrfCookieName)
      const sec = btoa(window.screen.width.toString())
      const realUserScore = isCrawler() ? 0 : 100

      if (customerId) {
        resolve(customerId)
      } else if (csrftoken) {
        resolve(csrftoken)
      } else {
        setTimeout(() => {
          if (CookieService.get(customerIdCookieName)) {
            return
          }

          ApiService.createToken({
            clientId,
            csrftoken,
            sec,
            realUserScore,
          }).then((response: MixedObject) => {
            if (!csrftoken) {
              CookieService.set(csrfCookieName, response.csrftoken, {
                expires: 86400 * 7, // 7 days
              })

              resolve(response.csrftoken)
            }
          })
        }, csrfAfter)
      }
    })
  }

  static deleteToken() {
    CookieService.delete(csrfCookieName)
  }
}