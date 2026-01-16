import {Injectable, provide} from 'angular2/core';
import {Observable} from 'rxjs/Observable';
import {Http, Response, RequestOptions, Headers, HTTP_PROVIDERS} from 'angular2/http';
import {status, json} from '../utils/fetch';
import 'rxjs/Rx';
import {AuthHttp, JwtHelper} from 'angular2-jwt';
import {AuthService} from'./AuthService';
//let jwtDecode = require('jwt-decode');


@Injectable()
export class UserService {
	static BASE_URL: string = 'http://localhost:3000/api';
	jwtHelper: JwtHelper = new JwtHelper();
	header: Headers;
	opts: RequestOptions;
	
	constructor(public http: Http, public authHttp: AuthHttp, public _authService: AuthService) {
		this.header = new Headers();
		this.header.append('Content-Type', 'application/json');
		this.opts = new RequestOptions();
   		this.opts.headers = this.header;
	}
	
	getUser(): any {
		 if(this._authService.logged === true) {
			let _token = localStorage.getItem('id_token');
			let decode = this.jwtHelper.decodeToken(_token).__data;
			console.log('get user decode', decode);
			var userId = decode.userId;
			return this.authHttp.get(`${UserService.BASE_URL}/Users/`+userId, { headers: this.header} )
				.do(
				data => console.log(json(data)),
				err => {
					console.log('error');
			},
				() => console.log('Request Complete')
				);
		 }
	}

}