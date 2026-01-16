import {
  BaseMapProvider,
  Interfaces,
} from 'react-modular-map';

import {
  autobind,
  override,
} from 'core-decorators';

export interface IOptionArgs {
  apiKey: String;
}

export class DaumMapProvider extends BaseMapProvider {
  constructor(options: IOptionArgs) {
    super(options);
  }
  @override
  @autobind
  initialize(domNode: HTMLElement, options: any) {

  }
  @override
  @autobind
  setDimensions(dimension: Interfaces.IDimension) {

  }
  @override
  @autobind
  __setCenter(center: Interfaces.ILatLng) {

  }
  @override
  @autobind
  __setZoom(zoomLevel: Number) {

  }

  @override
  @autobind
  onBoundsChanged(handler): any {

  }

  @override
  @autobind
  onZoomChanged(handler): any {

  }

  @override
  @autobind
  onCenterChanged(handler): any {

  }

  @override
  @autobind
  getCenter(): Interfaces.ILatLng {

  }

  @override
  @autobind
  getZoomLevel(): Number {

  }

  @override
  @autobind
  pointToLatLng(point: Interfaces.IPoint): Interfaces.ILatLng {

  }

  @override
  @autobind
  latLngToPoint(latlng: Interfaces.ILatLng): Interfaces.IPoint {

  }
  setCenter(center: Interfaces.ILatLng): Promise<void>;
  setZoom(zoomLevel: Number): Promise<void>;
}