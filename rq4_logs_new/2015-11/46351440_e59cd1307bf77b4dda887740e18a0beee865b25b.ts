/// <reference path="../typings/angular2-meteor.d.ts" />

import {NgZone, Component, View, NgFor} from 'angular2/angular2';
import {bootstrap} from 'angular2-meteor';
import {Parties} from 'collections/parties';
import {PartiesForm} from 'client/parties-form/parties-form'


@Component({
	selector: 'app'	
})
@View({
	templateUrl: 'client/app.html',
	directives: [NgFor, PartiesForm]
})
class SimpleChat {
    public parties: Mongo.Cursor<Object>;
    
	constructor(zone: NgZone) {
            this.parties = Parties.find();
	}
	
	
	removeParty(party) {
		Parties.remove(party._id);
	}
}

bootstrap(SimpleChat);