import {HttpClient, HttpParams} from "@angular/common/http";
import {Injectable} from "@angular/core";
import {Page} from "../shared/page.model";
import {MapLocation} from "./map-location.model";
import {Route} from "../route/route.model";
import {backendUrl} from "../shared/http.config";


@Injectable({providedIn: 'root'})
export class MapLocationService {

  constructor(private http: HttpClient) {}
  getMapLocationsByRoute(pageNumber: number, pageSize: number, route: Route) {
    let params = new HttpParams()
      .set('pageNumber', pageNumber.toString())
      .set('pageSize', pageSize.toString());

    const url = `${backendUrl}routes/${route.id}/mapLocations`
    return this.http.get<Page<MapLocation>>(url, { params });
  }

}