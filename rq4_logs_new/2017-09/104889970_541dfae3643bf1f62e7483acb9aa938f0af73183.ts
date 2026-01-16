import{Injectable} from '@angular/core';
import{Http , Response} from '@angular/http';
import 'rxjs/add/operator/map';
@Injectable()
export class ApiService{
	constructor(private http:Http){
	}
	register(value:any){
		return this.http.post('http://localhost:4000/api/register',value).map(res=>res.json());
	}
	login(value:any){
		return this.http.post('http://localhost:4000/api/login',value).map(res=>res.json());
	}
	getUsers(){
		return this.http.get('http://localhost:4000/api/getusers').map(res=>res.json());
	}
	createroom(data:any){
		return this.http.post('http://localhost:4000/api/createroom',data).map(res=>res.json());
	}
	
}