import {Component, OnInit} from 'angular2/core';
import {KnightComponent} from "./knight.component";
import {KnightService} from "./knight.service";
import {Knight} from "./knight";

@Component({
    selector: 'knight-list',
    template: `
        <ul>
            <li *ngFor="#knight of knights"
                (click)="onSelect(knight)"   
                [class.clicked]="selectedKnight === knight"
                style="cursor: pointer"
            >Knight {{knight.name}} </li>
        </ul>
        <knight [knight]="selectedKnight"> </knight>
    `,
    directives: [KnightComponent],
    providers: [KnightService],
    styleUrls: ["../src/css/knight-list.component.css"]
})
export class KnightListComponent implements OnInit{
    public knights: Knight[];
    public selectedKnight = {};

    constructor(private _knightService: KnightService) {}

    
    
    
    ngOnInit():any {
        this.getKnights();
    }
    
    onSelect(knight) {
        this.selectedKnight = knight;
    }

    getKnights() {
        this._knightService.getKnights().then((knights: Knight[]) => this.knights = knights);
    }

    
}