import {Component, OnInit} from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators} from '@angular/forms';
import {Http, RequestOptions} from '@angular/http';
import {TokenService} from '../../service/token.service';
import {Router} from '@angular/router';
import 'rxjs/Rx';
import {ShareService} from '../../service/share.service';
import swal from 'sweetalert2';
import { environment } from '../../../environments/environment';

@Component({
  moduleId: module.id,
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css'],
  providers: [TokenService]
})
export class LoginComponent implements OnInit {
  email: string;
  data: any;
  loginForm: FormGroup;
  constructor(private formBuilder: FormBuilder,
              private http: Http,
              private tokenService: TokenService,
              private service: ShareService,
              private router: Router) {
    this.loginForm = this.formBuilder.group({
      email: new FormControl('', [Validators.required]),
      password: new FormControl('', [Validators.required])
    });
  }
  ngOnInit() {
  }
  /** Login system */
  login(model: any) {
    let data;
    data = {
      'grant_type': 'password',
      'client_id': environment.client_id,
      'client_secret': environment.client_secret,
      'username': model.email,
      'password': model.password,
      'scope': ''
    };
    let headers, options;
    headers = new Headers();
    headers.append('Content-Type', 'application/json');
    headers.append('Accept', 'application/json');
    options = new RequestOptions({headers: headers});
    this.http.post(environment.hostname + '/oauth/token', data, options).map(res => res.json()).subscribe((a: any) => {
      this.tokenService.setToken(a);
      this.service.loginToken(a.access_token);
      swal('Thông báo', 'Đăng nhập thành công!', 'success');
      this.router.navigate(['/home']);
    }, (err: any) => {
      if (err.status === 401) {
        swal('Thông báo', 'Email hoặc mật khẩu không tồn tại!', 'error');
      } else {
        swal('Thông báo', 'Đăng nhập thất bại!', 'error');
      }
    });
  }
}