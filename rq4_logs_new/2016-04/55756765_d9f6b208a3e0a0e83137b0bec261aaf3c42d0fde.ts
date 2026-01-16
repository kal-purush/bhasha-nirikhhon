import 'reflect-metadata';
import 'zone.js/dist/zone';
import {Component, NgZone} from 'angular2/core';
import {bootstrap} from 'angular2-meteor-auto-bootstrap';
import {Parties} from '../collections/parties.ts';

@Component({
    selector: 'app',
    templateUrl: 'client/app.html'
})
class Socially {
    parties: Mongo.Cursor;

    constructor (zone: NgZone) {
        Tracker.autorun(() => zone.run(() => {
            this.parties = Parties.find();
        }));
    }
}

bootstrap(Socially);