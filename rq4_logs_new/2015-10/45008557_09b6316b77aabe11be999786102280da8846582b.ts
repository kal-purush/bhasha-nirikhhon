/// <reference path="../../typings/angular2-meteor.d.ts" />

import {Component, View, NgFor} from 'angular2/angular2';
import {RouterLink} from 'angular2/router';
import {Pages} from 'collections/pages';
import {PagesForm} from 'client/admin/pages-form';
@Component({
	selector: 'app'
})
@View({
	templateUrl: 'client/public/nav.html',
	directives: [NgFor,PagesForm,RouterLink]
})
export class Nav {
	pages: Mongo.Cursor<Object>;

	constructor () {

			this.pages = Pages.find();

	}
	removePage(page){
		console.log(page._id);
		  Pages.remove(page._id);
	}
}