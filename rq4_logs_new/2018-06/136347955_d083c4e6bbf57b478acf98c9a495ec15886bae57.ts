import { Component, EventEmitter, Input, ViewChild, OnInit } from '@angular/core'; //EventEmitter sends data between two components
import { NgForm } from '@angular/forms';
import { User } from '../shared/user.model';
import { Entry } from '../shared/entry.model';

@Component({
    selector: 'query-result-table',
    templateUrl: 'query-result-table.component.html',
    styleUrls: ['query-result-table.component.css']
})

export class QueryResultTableComponent implements OnInit{
    @Input() user: User //Receiving this user from the input text selection

    entriesFromUser: Entry[]

    ngOnInit()
    {
        this.entriesFromUser = this.user.entries
    }
}