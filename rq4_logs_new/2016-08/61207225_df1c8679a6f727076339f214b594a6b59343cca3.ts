import {Component, NgZone, Input} from "@angular/core";
import * as _ from 'lodash';

@Component({
    selector: '[grid-view]',
    templateUrl: 'Components/GridView/grid-view.tpl.html'
})

export class GridView {
    @Input() gvOptions: any = {
        data: [],
        columnDefs: []
    };
    private _coldefs: any = [];
    constructor(private zone: NgZone) { }

    ngOnInit() {
        // this.zone.run(() => {
        //     if (!this.gvOptions.columns || this.gvOptions.columns.length === 0) {
        //         $.each(this.gvOptions.data, function (index, value) {
        //             console.log(index);
        //             console.log(value);
        //         })
        //     }
        // });
    }
}
