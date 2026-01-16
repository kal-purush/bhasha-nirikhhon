import{Injectable} from '@angular/core';
import{Http , Response} from '@angular/http';
import 'rxjs/add/operator/map';
@Injectable()
export class ApiService{
	constructor(private http:Http){
	}
	getData(){
		return this.http.get('http://localhost:3000/api/contacts').map(res=>res.json());
	}
	register(value:any){
		return this.http.post('http://localhost:3000/api/register',value).map(res=>res.json());
	}
	login(value:any){
		return this.http.post('http://localhost:3000/api/login',value).map(res=>res.json());
	}
	contactData(){
		return this.http.get('http://localhost:3000/api/contacts').map(res=>res.json());
	}
}