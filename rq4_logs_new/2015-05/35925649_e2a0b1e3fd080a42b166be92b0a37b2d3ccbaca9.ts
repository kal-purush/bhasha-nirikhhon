///<references path="../typings/angular2/angular2.d.ts">

import {Component, View, bootstrap} from 'angular2/angular2';



module components{

	//header
	export function header (title,logo) {
		@Component({selector: 'my-header'});
		@View({
		  template: '<header class="my-header">'+
		  				'<div class="title">{{title}}</div>'+
		  				'<img src="{{logo}}" class="logo">/'+
		  			'</header>'
		});
		// Component controller
		class myHeaderComponent {
		  title: string;
		  logo: string;
		  
		  constructor() {
		    this.title 	= title;
		    this.logo	= logo;
		  }
		}

		bootstrap(myHeaderComponent);

	};


	/*//nav bar
	export function navBar (navList) {
		@Component({
		  selector: 'my-nav'
		})
		@View({
		  template: '<nav>'+
		  				'<ul>'+
		  					'<li ng-repeat="nav in navList">'+
		  						'<a href="{{nav.link}}">{{nav.title}}</a>'+
		  					'</li>'+
		  			'</nav>'
		})
		// Component controller
		class MyNavComponent {
		  navList			: array;
		  
		  constructor() {
		    this.navList 	= navList;
		  }
		}
		bootstrap(MyNavComponent);

	}


	//table
	export function table (columnHeaders,rows) {
		@Component({
		  selector: 'my-table'
		})
		@View({
		  template: '<table>'+
		  				'<thead>'+
		  					'<td ng-repeat="colName in columnHeaders">{{colName}}</td>'+
		  				'</thead>'+
		  				'<tbody>'+
		  					'<tr ng-repeat="row in rows">'+
		  						'<td ng-repeat="colName in columnHeaders">{{$parent.row[colName]}}</td>'+
		  					'</tr>'+
		  				'</tbody>'+
		  			'</table>'
		})
		// Component controller
		class MyTableComponent {
		  columnHeaders	: array;
		  rows			: array;
		  
		  constructor() {
		    this.columnHeaders 	= columnHeaders;
		    this.rows 			= rows;
		  }
		}


		bootstrap(MyTableComponent);

	}*/

}

