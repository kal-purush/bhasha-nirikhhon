import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
	providedIn: 'root'
})
export class DataFetchingService {

	baseUrlHistory:string = "https://api.exchangeratesapi.io/history";
	baseUrlLatest:string = "https://api.exchangeratesapi.io/latest";
	
	constructor(private request: HttpClient) { }

	fetchCurrencyData(history:boolean, params?:any):Observable<object>{
		if(history){
			if(!params.startDate){
				throw new Error("Start Date Missing");
			}
			return this.request.get(this.baseUrlHistory, {params:params});
		}	
		else{
			return this.request.get(this.baseUrlLatest, {params:params});
		}
	}
}