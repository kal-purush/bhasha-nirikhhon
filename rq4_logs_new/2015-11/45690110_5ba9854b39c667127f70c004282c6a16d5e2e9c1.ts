import {Component, View, bootstrap, NgFor} from 'angular2/angular2';
import {HTTP_PROVIDERS, Http} from 'angular2/http';
@Component({
    selector: 'my-app',
    providers: [HTTP_PROVIDERS]    
})
@View({
        directives: [NgFor],
        templateUrl : 'city.html' 
    })
class MyAppComponent {
    locations: Object;
    city: Object;
    hrequest: Http;
    
    constructor(http: Http) {
        http.get('http://www.findfreewifi.co.za/publicjson/getcities').subscribe(res => {
            this.locations = res.json().data;       
        });
        this.hrequest = http;    
    } 

    displayNearBy(obj: Object) {
        debugger;
        var link = 'http://www.findfreewifi.co.za/publicjson/Locations?cityName=' + obj.Name;
        this.hrequest.get(link).subscribe(res => {
            this.city = res.json().data;
        }); 

    }
    
      
}


bootstrap(MyAppComponent);