import {bootstrap, Component, FORM_DIRECTIVES, NgFor} from 'angular2/angular2';
import {Http, HTTP_PROVIDERS} from 'angular2/http';

console.log('done start')
@Component({ 
    selector: 'my-app', 
    viewProviders: [HTTP_PROVIDERS],
    directives: [FORM_DIRECTIVES, NgFor]
    templateUrl: 'wifi.html'
})
class AppComponent { 
	cities = []
	spots = []
	$http = null
	selectCity(evt, city){
		console.log("selected:", city)
		$http.get('http://www.findfreewifi.co.za/publicjson/Locations?cityName=' + city.Name)
	      	// Call map on the response observable to get the parsed people object
	      	.map(res => res.json())
	      // Subscribe to the observable to get the parsed people object and attach it to the
	      // component
	      .subscribe(response => {
	      	this.spots = response.data
	      })
	}
    constructor(http: Http) { 
    	$http = http

    	$http.get('http://www.findfreewifi.co.za/publicjson/getcities')
      	// Call map on the response observable to get the parsed people object
      	.map(res => res.json())
      // Subscribe to the observable to get the parsed people object and attach it to the
      // component
      .subscribe(response => {
      	this.cities = response.data
      	console.log(this.cities)
      })
  }
}
console.log('mid')
bootstrap(AppComponent);

console.log('done end')