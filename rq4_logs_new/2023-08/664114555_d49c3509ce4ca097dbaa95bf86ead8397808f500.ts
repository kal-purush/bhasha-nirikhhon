import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { LoanForm } from '../Models/LoanForm';


@Injectable({
  providedIn: 'root'
})
export class LoanFormDataShareService {
  private _loanForm:BehaviorSubject<LoanForm> = new BehaviorSubject<LoanForm>({
    amount: 0,
    repaymentPeriodCount: 0,
    repaymentPeriodUnit: '',
    loanProductId: 0,
    name:'',
    nameProduct:'',
    typeAmortization:'',
  });
  constructor() { }

  get loanForm$():Observable<LoanForm>{
    return this._loanForm.asObservable();
  }

  set loanForm(newLoanForm:LoanForm){
    this._loanForm.next(newLoanForm);
  }
}