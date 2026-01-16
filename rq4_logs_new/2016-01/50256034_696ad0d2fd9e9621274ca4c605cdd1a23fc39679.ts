/**
 * Created by michalzagrodzki on 23/01/16.
 */

import {Component} from 'angular2/core';
import {Writer} from './writer';
import {WriterDetailComponent} from './writer-detail.component';
import {WriterService} from './writer.service';
import {WRITERS} from './writer.service';
import {OnInit} from 'angular2/core';

@Component({

    /** html selector **/
    selector: 'writer-name',

    /** css styles **/
    styles:[`

        .selected {
        background-color: #CFD8DC !important;
        color: white;
      }
      .writers {
        margin: 0 0 2em 0;
        list-style-type: none;
        padding: 0;
        width: 10em;
      }
      .writers li {
        cursor: pointer;
        position: relative;
        left: 0;
        background-color: #EEE;
        margin: .5em;
        padding: .3em 0em;
        height: 1.6em;
        border-radius: 4px;
      }
      .writers li.selected:hover {
        color: white;
      }
      .writers li:hover {
        color: #607D8B;
        background-color: #EEE;
        left: .1em;
      }
      .writers .text {
        position: relative;
        top: -3px;
      }
      .writers .badge {
        display: inline-block;
        font-size: small;
        color: white;
        padding: 0.8em 0.7em 0em 0.7em;
        background-color: #607D8B;
        line-height: 1em;
        position: relative;
        left: -1px;
        top: -4px;
        height: 1.8em;
        margin-right: .8em;
        border-radius: 4px 0px 0px 4px;
      }

    `],

    /** html template for presenting data **/
    template: `
    <h1>{{title}}</h1>

    <h2>My Writers</h2>
    <ul class="writers">
    <li *ngFor="#writer of writers"
        [class.selected]="writer === selectedWriter"
        (click)="onSelect(writer)">
        <span class="badge">{{writer.id}}</span> {{writer.name}}
    </li>
    </ul>
    <my-writer-detail [writer]="selectedWriter"></my-writer-detail>
    `,
    directives: [WriterDetailComponent],

    providers: [WriterService]

})

export class AppComponent implements OnInit {

    public title = 'Tour of writers';

    public writers: Writer[];

    public selectedWriter: Writer;

    onSelect(writer: Writer) { this.selectedWriter = writer }

    /** private class for writerService **/
    constructor(private _writerService: WriterService) {
    }

    /** function to call data through private service variable **/
    getWriters() {
        /** async call with delayed assignment of data to writers **/
       this._writerService.getWriters()
        .then(writers => this.writers = writers);
    }

    /** load items on initialization **/
    ngOnInit() {
        this.getWriters();
    }

}

