import { OnInit,Component } from '@angular/core';
import{Http,Response,Headers} from '@angular/http';
import 'rxjs/add/operator/map';
// import { HttpClient } from '@angular/common/http';
import { HttpHeaders } from '@angular/common/http';

@Component({
  selector: 'login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css']


})
export class LoginComponent{
  name : String;
  password : String;

  constructor(public http: Http) {
    
      }
     
      login() {
       const that = this;
       console.log(this.name);
        this.http.post('http://127.0.0.1:4500/login',{
          username: that.name,
          password: that.password
        })
      .subscribe(
        data => {
          alert('ok');
          console.log("------------------>data is ",data)},
        error => {
          console.log(error , "erorr in login ");
        }
        )

    }

}