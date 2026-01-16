import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { MapLayerData, mapLayerDataList } from '../component/map-layer/map-layer.data';

@Injectable({
  providedIn: 'root',
})
export class MapLayerService {
  mapLayerDataList$ = new BehaviorSubject<MapLayerData[]>(mapLayerDataList);

  getMapLayerData(): Observable<MapLayerData[]> {
    return this.mapLayerDataList$.asObservable();
  }

  /**
   * Update the map layer data list to trigger the onPush event.
   */
  updateMapLayerData(): void {
    this.mapLayerDataList$.next(mapLayerDataList);
  }
}