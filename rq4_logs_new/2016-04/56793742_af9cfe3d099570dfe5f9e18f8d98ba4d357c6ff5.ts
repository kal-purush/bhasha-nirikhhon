/**
 * Created by zorodzay on 2016/04/25.
 */
import {Injectable, Inject} from "angular2/core";
import {Http, Headers} from "angular2/http";
import "rxjs/add/operator/map";

@Injectable()
export class TenderFraudComplaintService {
    constructor(@Inject(Http)private http:Http) {
    }

    save(tenderFraudComplaint):any {
        var headers = new Headers();
        headers.append('Content-Type', 'application/json');
        return this.http.post('/tenderFraudComplaints', JSON.stringify(tenderFraudComplaint),{headers:headers}).map(complaint => complaint.json());
    }
}