/**
 * Created by tom on 23.06.16.
 */

import { Component, Input } from '@angular/core';
import { TranslatePipe } from 'ng2-translate/ng2-translate';

@Component({
    selector: 'vp-translate',
    template: `<span>{{ key | translate:getReplace() }}</span>`,
    pipes: [TranslatePipe]
})
export class VpTranslateComponent {
    @Input()
    private key:string;

    @Input()
    private replace:Array<string> = [];

    getReplace():any {
        const result = {};
        this.replace.forEach((actual, i) => {
            result['$' + i] = actual;
        });
        return result;
    }
}