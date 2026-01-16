import {Component, Input, Output, EventEmitter, OnInit, DoCheck} from "@angular/core";
import {DataTable} from "./DataTable";

@Component({
    selector: "mfRowSelector",
    template: `
        <input type="checkbox" [checked]="isChecked" (change)="onChange($event)" />
        `
})
export class RowSelector implements OnInit, DoCheck {
    @Output("selectEntity") rowSelected = new EventEmitter();
    @Input("entity") private rowEntity: any = Object;
    @Input("selectedEntities") private selectedEntities: any[];

    private isChecked: boolean = false;

    public constructor() {
    }

    public ngOnInit() {
        this.getIsChecked();
    }

    public ngDoCheck() {
        this.getIsChecked();
    }

    private getIsChecked() {
        if (this.selectedEntities != null) {
            let index = this.selectedEntities.indexOf(this.rowEntity);
            this.isChecked = index > -1;
        }
    }

    private onChange($event) {
        this.isChecked = !this.isChecked;
        this.rowSelected.emit(this.rowEntity);
    }
}