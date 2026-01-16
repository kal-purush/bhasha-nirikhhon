import {Injectable, provide} from 'angular2/core';
import {Observable} from 'rxjs/Observable';
import {Router, RouterLink} from 'angular2/router';
import {Http, Response, RequestOptions, Headers, HTTP_PROVIDERS} from 'angular2/http';
import {status, json} from '../utils/fetch';
import 'rxjs/Rx';
import {AuthHttp, JwtHelper, tokenNotExpired} from 'angular2-jwt';

@Injectable()
export class AuthService {
	static BASE_URL: string = 'http://localhost:3000/api';
	jwtHelper: JwtHelper;
	header: Headers;
	opts: RequestOptions;
	logged: boolean;
	
	constructor(public http: Http, 
				public authHttp: AuthHttp, 
				public router: Router) {
		this.header = new Headers();
		this.jwtHelper = new JwtHelper();
		this.header.append('Content-Type', 'application/json');
		this.opts = new RequestOptions();
   		this.opts.headers = this.header;
		this.logged;   
	}
	
	login(username: string, password: string): any {
		
		let body = {
			email: username,
			password: password
		};
		
		return this.http.post(`${AuthService.BASE_URL}/Users/login`, JSON.stringify(body), { headers: this.header} )
		.do( (res) => {
			console.log('res', json(res));
			let r = json(res);
			localStorage.setItem('id_token', r.token);
			this.logged = true;
			this.router.navigate(['/Protected']);
			console.log('loggedIn is, ', this.logged);
			console.log('helloooooooooo guyssssss');
		} )
	}
	
	getUser(): any {
		let _token = localStorage.getItem('id_token');
		let decode = this.jwtHelper.decodeToken(_token).__data;
		console.log('get User decode', decode);
		let userId = decode.userId;
		return this.authHttp.get(`${AuthService.BASE_URL}/Users/`+userId, { headers: this.header} )
			.do(
			data => console.log(json(data)),
			err => {
				console.log('error');
		},
			() => console.log('Request Complete')
			);
	}
	
	logout(): any {
		
		
		let jwt = localStorage.getItem('id_token');
		var lb_token = {
			access_token: jwt
		}
		return this.authHttp.post(`${AuthService.BASE_URL}/Users/logout`, JSON.stringify(lb_token), { headers: this.header} )
			.do(
			() => {
			// Remove JWT from local storage
			localStorage.removeItem('id_token');
			this.logged = false;
			this.router.navigate(['/Home']);
			console.log('loggedIn is, ', this.logged);
			console.log('logout succesfully!');			
		},
			err => console.log(err),
			() => console.log('Request Complete')
			);
	}
	
	register(username: string, password: string): any {
		let newUser = {
			email: username,
			password: password
		};
		
		return this.http.post(`${AuthService.BASE_URL}/Users`, 
			JSON.stringify(newUser), this.opts)
			.do(res => {
				let r = json(res);
				//console.log(r);
			}, err => {
				console.log('Something went wrong. Provide correct credentials to register succesfully!');
			});
	}
	
	verifyLogged() {
		
		if( localStorage.getItem('id_token')) {
			let jwt = localStorage.getItem('id_token');
			console.log('jwt', jwt);
			let jwtHelper = new JwtHelper();
			let expired = this.jwtHelper.isTokenExpired(jwt);
			//console.log(expired);
			if(expired) {
				//return false; 
				this.logged = false;
				console.log('thisLogged', this.logged);
			}
			else{
				//return true;
				this.logged = true; 
				console.log('thisLogged', this.logged);
			}
		}else {
			//return false;
			this.logged = false;
			console.log('thisLogged', this.logged);
		}
		
	}
	
	check() {
		console.log(Observable.of(this.logged));
		return Observable.of(this.logged);
	}
	
}