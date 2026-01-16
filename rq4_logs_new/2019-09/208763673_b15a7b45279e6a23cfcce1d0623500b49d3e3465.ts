import { Injectable } from '@angular/core';
//-------------------------------------------------------
@Injectable({
  providedIn: 'root'
})
export class ClassmanagerService {
  constructor() { }
}
//-------------------------------------------------------
// class used to pull all data from test table in database
//------------------test--------------------------
@Injectable({
  providedIn: 'root'
})
export class test {
constructor(
first: string,
last: string
 ) {}
}
//-------------------------------------------------------