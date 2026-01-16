import { stringify as stringifyParsedQuery } from 'querystring'
import { Base } from './Base'
import {
  RestaurantsSearchQuery,
  Restaurant,
  StocksSearchQuery,
  Stock,
  Auth,
} from './index.d'

export class ReMap extends Base {
  public async getRestaurants(query: RestaurantsSearchQuery): Promise<Restaurant[]> {
    const res = await this.get(`${this.RESTAURANTS_SERVICE_ORIGIN}/?${stringifyParsedQuery(query)}`)
    return await res.json()
  }

  public async getRestaurantById(id: string): Promise<Restaurant> {
    const res = await this.get(`${this.RESTAURANTS_SERVICE_ORIGIN}/${id}`)
    return await res.json()
  }

  public async createStock(restaurantId: string, token: string): Promise<{ location: string }> {
    const res = await this.post(this.STOCKS_SERVICE_ORIGIN, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ restaurant_id: restaurantId }),
    })

    return { location: res.headers.get('Location') }
  }

  public async getStocks(query: StocksSearchQuery, token: string): Promise<Stock[]> {
    const queryString: string = stringifyParsedQuery(query)
    const param: string = queryString ? `?${queryString}` : ''
    const res = await this.get(`${this.STOCKS_SERVICE_ORIGIN}${param}`, {
      headers: { Authorization: `Bearer ${token}` },
    })

    return await res.json()
  }

  public async getStockById(id: string, token: string): Promise<Stock> {
    const res = await this.get(`${this.STOCKS_SERVICE_ORIGIN}/${id}`, {
      headers: { Authorization: `Bearer ${token}` },
    })
    return await res.json()
  }

  public async deleteStockById(id: string, token: string): Promise<void> {
    await this.delete(`${this.STOCKS_SERVICE_ORIGIN}/${id}`, {
      headers: { Authorization: `Bearer ${token}` },
    })
  }

  public async authenticate(idToken: string): Promise<Auth> {
    const res = await this.post(this.AUTHENTICATION_SERVICE_ORIGIN, {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id_token: idToken }),
    })
    
    return await res.json()
  }
}