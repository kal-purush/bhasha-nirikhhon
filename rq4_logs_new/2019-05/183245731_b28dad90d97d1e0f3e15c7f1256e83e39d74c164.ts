import { Http } from './Http'
import { Path } from './Path'
import { FeatureCollection, MultiPolygon } from 'geojson'
import { County, CountyProperties } from '../models/County'
import { isArray } from '../helpers/utils'

export class Counties {
  private http: Http

  constructor(http: Http) {
    this.http = http
  }

  /**
   * @param {string} abbreviation (NY, AK, AI, LA, KY, etc)
   * @return {Promise<Array<County>>}
   */
  async getByStateAb(abbreviation: string): Promise<Array<County>> {
    const stateAbURI: string = encodeURI(abbreviation)

    return this.http.get<Array<County>>(
      `${Path.COUNTIES_SEARCH_PATH}/byState/${stateAbURI}${Path.ENTITY_PATH}`,
      {}
    )
  }

  /**
   * @param {string} geoId
   * @return {Promise<FeatureCollection<MultiPolygon, CountyProperties>>}
   */
  async getGeojsonByGeoId(
    geoId: string
  ): Promise<FeatureCollection<MultiPolygon, CountyProperties>> {
    const geoIdURI = encodeURI(geoId)

    return this.http.get<FeatureCollection<MultiPolygon, CountyProperties>>(
      `${Path.COUNTIES_PATH}/${geoIdURI}`,
      {}
    )
  }

  /**
   * @param {string} geoId
   * @return {Promise<County>}
   */
  async getByGeoId(geoId: string): Promise<County> {
    const geoIdURI = encodeURI(geoId)

    return this.http.get<County>(`${Path.COUNTIES_PATH}/${geoIdURI}${Path.ENTITY_PATH}`, {})
  }

  /**
   * @param {string} countyName (County name Adair, etc)
   * @param {string} stateAb (NY, KY, AI, AL, etc)
   * @return {Promise<FeatureCollection<MultiPolygon, CountyProperties>>}
   */
  async getGeojsonByCountyNameAndStateAb(
    countyName: string,
    stateAb: string
  ): Promise<FeatureCollection<MultiPolygon, CountyProperties>> {
    const params = {
      name: countyName,
      stateAb: stateAb
    }

    return this.http.get<FeatureCollection<MultiPolygon, CountyProperties>>(
      `${Path.COUNTIES_SEARCH_PATH}${Path.BY_NAME_AND_STATE}`,
      params
    )
  }

  /**
   *
   * @param {string} countyName (County name Adair, etc)
   * @param {string} stateAb (NY, KY, AI, AL, etc)
   * @return {Promise<County>}
   */
  async getByCountyNameAndStateAb(countyName: string, stateAb: string): Promise<County> {
    const params = {
      name: countyName,
      stateAb: stateAb
    }

    return this.http.get<County>(
      `${Path.COUNTIES_SEARCH_PATH}${Path.BY_NAME_AND_STATE}${Path.ENTITY_PATH}`,
      params
    )
  }

  /**
   * @param {Array<string>} countyNames (Adair, etc)
   * @return {Promise<FeatureCollection<MultiPolygon, CountyProperties>>}
   */
  async getGeojsonByCountyNames(
    countyNames: Array<string>
  ): Promise<FeatureCollection<MultiPolygon, CountyProperties>> {
    isArray(countyNames)
    const params = {
      names: countyNames.join(',')
    }

    return this.http.get<FeatureCollection<MultiPolygon, CountyProperties>>(
      `${Path.COUNTIES_SEARCH_PATH}/byNames`,
      params
    )
  }

  /**
   * @deprecated
   * @param {Array<string>} abbreviations (NY,KY,AI, etc)
   * @return {Promise<FeatureCollection<MultiPolygon, CountyProperties>>}
   */
  async getGeojsonByStateAbs(
    abbreviations: Array<string>
  ): Promise<FeatureCollection<MultiPolygon, CountyProperties>> {
    isArray(abbreviations)
    const params = {
      statesAb: abbreviations.join(',')
    }

    return this.http.get<FeatureCollection<MultiPolygon, CountyProperties>>(
      `${Path.COUNTIES_SEARCH_PATH}/byStates`,
      params
    )
  }

  /**
   * @param {Array<number>} northEast (Coordinates of north-east corner [latitude, longitude] )
   * @param {Array<number>} southWest (Coordinates of south-west corner [latitude, longitude] )
   * @param {boolean} intersect (get counties that intersect with the bounding box)
   * @return {Promise<FeatureCollection<MultiPolygon, CountyProperties>>}
   */
  async getGeojsonInBoundingBox(
    northEast: Array<number>,
    southWest: Array<number>,
    intersect: boolean = true
  ): Promise<FeatureCollection<MultiPolygon, CountyProperties>> {
    isArray(northEast)
    isArray(southWest)
    const params = {
      intersect: intersect.toString(),
      northEast: northEast.join(','),
      southWest: southWest.join(',')
    }

    return this.http.get<FeatureCollection<MultiPolygon, CountyProperties>>(
      `${Path.COUNTIES_SEARCH_PATH}${Path.IN_BOUNDING_BOX_PATH}`,
      params
    )
  }

  /**
   * @param {Array<number>} northEast (Coordinates of north-east corner [latitude, longitude] )
   * @param {Array<number>} southWest (Coordinates of south-west corner [latitude, longitude] )
   * @param {boolean} intersect (get counties that intersect with the bounding box)
   * @return {Promise<Array<County>>}
   */
  async getInBoundingBox(
    northEast: Array<number>,
    southWest: Array<number>,
    intersect: boolean = true
  ): Promise<Array<County>> {
    isArray(northEast)
    isArray(southWest)
    const params = {
      intersect: intersect.toString(),
      northEast: northEast.join(','),
      southWest: southWest.join(',')
    }

    return this.http.get<Array<County>>(
      `${Path.COUNTIES_SEARCH_PATH}${Path.IN_BOUNDING_BOX_PATH}${Path.ENTITY_PATH}`,
      params
    )
  }

  /**
   * @param {number} latitude
   * @param {number} longitude
   * @param {number} radius (size in miles min values is 1 max 500)
   * @return {Promise<FeatureCollection<MultiPolygon, CountyProperties>>}
   */
  async getGeojsonInRadius(
    latitude: number,
    longitude: number,
    radius: number
  ): Promise<FeatureCollection<MultiPolygon, CountyProperties>> {
    const params = {
      latitude: latitude.toString(),
      longitude: latitude.toString(),
      radius: radius.toString()
    }

    return this.http.get<FeatureCollection<MultiPolygon, CountyProperties>>(
      `${Path.COUNTIES_SEARCH_PATH}${Path.IN_RADIUS_PATH}`,
      params
    )
  }

  /**
   * @param {number} latitude
   * @param {number} longitude
   * @param {number} radius (size in miles min values is 1 max 500)
   * @return {Promise<Array<County>>}
   */
  async getInRadius(latitude: number, longitude: number, radius: number): Promise<Array<County>> {
    const params = {
      latitude: latitude.toString(),
      longitude: latitude.toString(),
      radius: radius.toString()
    }

    return this.http.get<Array<County>>(
      `${Path.COUNTIES_SEARCH_PATH}${Path.IN_RADIUS_PATH}${Path.ENTITY_PATH}`,
      params
    )
  }
}