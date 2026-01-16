import { Injectable } from '@angular/core';
import { Bill } from './bill';
import { Http, Response } from '@angular/http';
import 'rxjs/add/operator/toPromise';


@Injectable()
export class BillService {

  private billsUrl = 'api/bills';
  
    constructor(private http: Http) { }
  
    createBill(newBill: Bill): Promise<void | Bill> {
      return this.http.post(this.billsUrl, newBill)
            .toPromise()
            .then(response => response.json() as Bill)
            .catch(this.handleError);
    }

    authenticatePin(bill_id: string, pin: string): Promise<void | Bill> {
      return this.http.post(this.billsUrl + '/' + bill_id, pin)
            .toPromise()
            .then(response => response.json() as Bill)
            .catch(this.handleError);
    }

    updateBill(putBill: Bill): Promise<void | Bill> {
      var putUrl = this.billsUrl + '/' + putBill._id;
      return this.http.put(putUrl, putBill)
                 .toPromise()
                 .then(response => response.json() as Bill)
                 .catch(this.handleError);
    }
  
  
    private handleError (error: any) {
      let errMsg = (error.message) ? error.message :
      error.status ? `${error.status} - ${error.statusText}` : 'Server error';
      console.error(errMsg); 
    }

}