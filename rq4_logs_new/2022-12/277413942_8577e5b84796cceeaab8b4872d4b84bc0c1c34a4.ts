import { Loader } from '@googlemaps/js-api-loader'

export type GoogleMapsApi = typeof google

export class GoogleMapsApiLoader {
  public api: typeof google

  async load() {
    const loader = new Loader({
      apiKey: process.env.GOOGLE_MAPS_KEY,
      version: 'weekly',
      libraries: ['places']
    })
    this.api = await loader.load()
  }
}