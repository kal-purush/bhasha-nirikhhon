import {
  autobind,
  override,
} from 'core-decorators';

import * as _ from 'lodash';

import {
  BaseMapProvider,
  Interfaces,
} from 'react-modular-map';

import {
  loadAPI, getDaumMapAPI,
} from './api-loader';

export interface IConstructorOpts {
  APIKey: String;
}

export default class DaumMapProvider extends BaseMapProvider {
  map: any;
  apiLoadPromise: any;
  constructor(options: IConstructorOpts) {
    super(options);
    this.apiLoadPromise = loadAPI(options.APIKey);
  }

  async initialize(domNode: HTMLElement, options) {
    // options 에서 initial position 받기.
    const mapApi = await this.apiLoadPromise;
    const center = options.center;
    const daumCenter = new mapApi.LatLng(center.lat, center.lng);
    const mapOptions = Object.assign({}, options, {
      center: daumCenter,
    });
    this.map = new mapApi.Map(domNode, mapOptions);
    this.initDefer.resolve();
  }

  setDimensions(dimension: Interfaces.IDimension) {
    debugger;
    this.map.relayout();
  }

  __setCenter(center: Interfaces.ILatLng) {
    const mapApi = getDaumMapAPI();
    const daumCenter = new mapApi.LatLng(center.lat, center.lng);
    this.map.setCenter(daumCenter);
  }

  __setZoom(zoomLevel: Number) {
    const mapApi = getDaumMapAPI();
    this.map.setLevel(zoomLevel);
  }


  onBoundsChanged(handler): any {
  }


  onZoomChanged(handler): any {

  }


  onCenterChanged(handler): any {

  }


  getCenter(): Interfaces.ILatLng {
    const mapApi = getDaumMapAPI();
    const daumCenter = mapApi.getCenter();
    return {
      lat: daumCenter.getLat(),
      lng: daumCenter.getLng(),
    };
  }


  getZoomLevel(): Number {
    const mapApi = getDaumMapAPI();
    return mapApi.getLevel();
  }


  pointToLatLng(point: Interfaces.IPoint): Interfaces.ILatLng {
    const mapApi = getDaumMapAPI();
    const projection = this.map.getProjection();
    const daumPoint = new mapApi.Point(point.left, point.top);
    const daumLatLng = projection.coordsFromPoint(daumPoint);
    return {
      lat: daumLatLng.getLat(),
      lng: daumLatLng.getLng(),
    };
  }


  latLngToPoint(latlng: Interfaces.ILatLng): Interfaces.IPoint {
    const mapApi = getDaumMapAPI();
    const projection = this.map.getProjection();
    const daumLatLng = new mapApi.LatLng(latlng.lat, latlng.lng);
    const daumPoint = projection.pointFromCoords(daumLatLng);
    return {
      left: daumPoint.x,
      top: daumPoint.y,
    };
  }
}