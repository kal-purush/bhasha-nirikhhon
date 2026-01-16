import { OnInit,Component } from '@angular/core';
import{Http,Response,Headers} from '@angular/http';
import 'rxjs/add/operator/map';
// import { HttpClient } from '@angular/common/http';
import { HttpHeaders } from '@angular/common/http';
@Component({
  selector: 'item',
  templateUrl: './item.component.html',
  styleUrls: ['./item.component.css']
})
export class ItemComponent {
  name : String;
  type : String;
  price:Number;
  massege:String;
  constructor(public http: Http) {
    
      }
     
  submit() {
  const that = this;
 console.log(that)
  this.http.post('http://localhost:4500/item',{
    itemname: that.name,
    price: that.price,
    massege : that.massege,
    itemtype : that.type
  })
  // JSON.stringify({
  //   username: that.name,
  //   password: that.password
  // })
.subscribe(
  data => {
    alert('ok');
    console.log(data)
  },
  error => {
    console.log(error , "erooooooooooooooooooe");
  }
  )

}
}

