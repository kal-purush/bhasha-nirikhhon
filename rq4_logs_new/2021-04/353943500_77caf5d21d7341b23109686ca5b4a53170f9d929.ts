
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class DataService {
  employeeData: Array<finalempView> = new Array<finalempView>();
  empData: finalempView = new finalempView();
  constructor() {
    
   }
}
export class EmpDataView{
  
  a:number = 0;
  b:number = 0;
  c:number = 0;
  d:number = 0;
  
  
}
export class EmpDetails{
  name: string;
  date: Date;
  upad: number=0;
  days: number=0;
  upadDate: Date;
  fixpagar: number=0;
}
export class Piece{
  total_A: number = 0;
  total_B: number = 0;
  total_C: number = 0;
  total_D: number = 0;
  total_All: number = 0;
  
}
export class Price{
  price_A: number = 0;
  price_B: number = 0;
  price_C: number = 0;
  price_D: number = 0;
}
export class Salary{
  salary_A: number = 0;
  salary_B: number = 0;
  salary_C: number = 0;
  salary_D: number = 0;
  salary_All: number = 0;
}

export class Total{
  totalsalary:number = 0;
  totalday:number = 0;
  daysalary:number = 0;
  earnsalary:number = 0;
  grandTotal:number = 0;
}

export class finalempView{
  empdata:EmpDataView[];
  empdetails:EmpDetails;
  piece: Piece;
  price: Price;
  salary : Salary;
  total:Total;

}