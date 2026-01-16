import {Injectable} from "@angular/core";
import {Place} from "../model/place.model";
import {BehaviorSubject} from "rxjs/BehaviorSubject";
import {Observable} from "rxjs/Observable";
import {Http} from "@angular/http";
import {SERVER_URL} from '../../config';
import {AuthHttp} from "angular2-jwt";
import 'rxjs/add/operator/do';

@Injectable()
export class PlaceService {

  public places$: BehaviorSubject<Place[]> = new BehaviorSubject([]);
  private _placeList: Place[];

  public selectedPlace: Place;

  constructor(private authHttp: AuthHttp) {
  }

  public loadPlaces(): Observable<any> {
    return this.authHttp.get(`${SERVER_URL}/places`).do((placeList) => {
      this._placeList = placeList.json();
      this.places$.next(this._placeList);
    });
  }

  public getPlace(placeId: number): Observable<Place> {
    return this.authHttp.get(`${SERVER_URL}/places/${placeId}`)
      .map(res => res.json());
  }

  public deletePlace(placeId: number): void {
    this.authHttp.delete(`${SERVER_URL}/places/${placeId}`)
      .subscribe(() => {
        this._placeList = this._placeList.filter(f => f.id != placeId);
        this.places$.next(this._placeList);
      });
  }

  public createPlace(place: Place): Observable<Place> {
    return this.authHttp.post(`${SERVER_URL}/places`, place)
      .map(res => res.json())
      .do(res => {
        this._placeList.push(Object.assign(new Place(), res));
        this.places$.next(this._placeList);
      });
  }

  public updatePlace(place: Place): Observable<Place> {
    return this.authHttp.put(`${SERVER_URL}/places/${place.id}`, place)
      .map(res => res.json())
      .do(res => {
        this._placeList.forEach((f, index) => {
          if (f.id === place.id) {
            this._placeList[index] = place;
          }
        });
        this.places$.next(this._placeList);
      });
  }

  public placeIsSelected(): boolean{
    return this.selectedPlace != null;
  }

}