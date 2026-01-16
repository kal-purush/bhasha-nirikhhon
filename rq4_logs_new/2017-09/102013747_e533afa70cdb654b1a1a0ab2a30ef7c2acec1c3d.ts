import {Injectable} from "@angular/core";
import {Http, Response} from "@angular/http";
import {Observable} from "rxjs/Observable";
import 'rxjs/add/operator/map'
import {Select2OptionData} from "ng2-select2";
import {Constants} from "../../constants";
@Injectable()
export class MbtiService {
  constructor(private _http: Http, private constant: Constants) {

  }


  getMbti(): Observable<any[]>{
    return this._http.get(this.constant.MBTI)
      .map((response: Response) => response.json())
  }
}
