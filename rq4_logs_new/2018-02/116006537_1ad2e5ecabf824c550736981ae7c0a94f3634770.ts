import { Event } from '../classes/Evento.class';
import { Usuario } from '../classes/Usuario.class';
import { Comentario } from '../classes/Comentario.class';
import { Tag } from '../classes/Tag.class';

import { Injectable } from '@angular/core';
import { Http, Response, JsonpModule, RequestOptions, Headers } from '@angular/http';
import { LoginService } from './login.service';
import { Router, ActivatedRoute } from '@angular/router';

import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/map';
import 'rxjs/Rx';
import { Classroom } from '../classes/Classroom.class';
import { error } from 'selenium-webdriver';

const BASE_URL = 'https://15psp95at5.execute-api.us-east-1.amazonaws.com/dev/talkclass/users'


@Injectable()
export class UsersService {
    const
    constructor(private http: Http, private router: Router){}

    createUser(username:string, firstname: string, role: string,lastname:string, password:string, birthday:string,email:string, address:string, postal_code :number,
        phone:number,classroom:string, photo_profile:string, photo_profile_name:string){
        let body = {
            role: role,
            username: username,
            first_name: firstname,
            last_name: lastname,
            birthday: birthday,
            email: email,
            address: address,
            postal_code: postal_code.toString(),
            phone: phone.toString(),
            password: password,
            photo_profile: photo_profile,
            photo_profile_name: photo_profile_name
            }
            console.log('body antes')
            console.log(body)
        if (role == 'parent'){
            body['classroom'] = classroom
        }
        if ((role == 'teacher') && (classroom)){
            body['tutor_class'] = classroom
        }
        console.log('body despues')
        console.log(body)
        return this.http.post(BASE_URL,body).map(
            response => {
                console.log("status creation user: "+response.status)
            },
            error => {console.error(error)}
        );
    }
    //--------------
  
    private handleError(error: any) {
		console.error(error);
		return Observable.throw("Server error (" + error.status + "): " + error.text());
	}
}