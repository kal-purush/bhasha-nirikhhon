import {Injectable} from '@angular/core';  
import {Http} from '@angular/http';

@Injectable()
export class MenuCallService {  
    public menuItems: any;

    constructor(private http: Http) {
        
    }

    public getMenu(category) {
        let menu = this.http.get(`https://keanubackend.herokuapp.com/item/category/${category}`).map(res => res.json()).subscribe(
        data => {
            this.menuItems = data.data.items
        },
        err => {
          console.log("Oops!");
        }
      );
        console.log("From GetMenu.ts " + this.menuItems);
        return this.menuItems;
    }
}