import {AfterContentChecked, Component, OnInit} from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators, AbstractControl} from '@angular/forms';
import {Http, RequestOptions} from '@angular/http';
import {TokenService} from '../../service/token.service';
import {Router} from '@angular/router';
import 'rxjs/Rx';
import {ShareService} from '../../service/share.service';
import swal from 'sweetalert2';
import {environment} from '../../../environments/environment';
import {TranslateService} from '@ngx-translate/core';

@Component({
    moduleId: module.id,
    selector: 'app-register',
    templateUrl: './register.component.html',
    styleUrls: ['./register.component.css'],
})
export class RegisterComponent implements OnInit {
    email: string;
    data: any;
    registerForm: FormGroup;
    responseData: any;

    constructor(private formBuilder: FormBuilder,
                private http: Http,
                private service: ShareService,
                private router: Router,
                private translate: TranslateService) {
        this.registerForm = this.formBuilder.group({
            full_name: new FormControl('', [Validators.required]),
            email: new FormControl('', [Validators.required, Validators.email]),
            password: new FormControl('', [Validators.required]),
            password_confirmation: new FormControl('', [])
        }, {
            validator: this.MatchPassword
        });
    }

    ngOnInit() {
    }

    /** Registered system */
    register(model: any) {
        let data;
        data = {
            'full_name': model.full_name,
            'email': model.email,
            'password': model.password,
            'password_confirmation': model.password_confirmation,
        };
        let headers, options;
        headers = new Headers();
        headers.append('Content-Type', 'application/json');
        headers.append('Accept', 'application/json');
        options = new RequestOptions({headers: headers});
        this.http.post(environment.hostname + '/api/users', data, options).map(res => res.json()).subscribe((resJson: any) => {
            this.responseData = resJson;
            swal(this.responseData.message, this.responseData.success, 'success');
            this.router.navigate(['/login']);
        }, (err: any) => {
            if (err.status === 422) {
                swal(this.responseData.message, this.responseData.success, 'error');
            } else {
                swal(this.responseData.message, this.responseData.success, 'error');
            }
        });
    }

    MatchPassword(AC: AbstractControl) {
        let password, confirmPassword;
        password = AC.get('password').value; // to get value in input tag
        confirmPassword = AC.get('password_confirmation').value; // to get value in input tag
        if (password !== confirmPassword) {
            AC.get('password_confirmation').setErrors({MatchPassword: true});
        } else {
            return null;
        }
    }
}