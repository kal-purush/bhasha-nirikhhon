import { Component, NgModule } from '@angular/core';
import { Docent } from '../docent/docent';
import { DocentaanmakenService } from './docentaanmaken.service';

@Component({
    selector: 'docentaanmaken',
    templateUrl: './docentaanmaken.component.html',
    providers: [DocentaanmakenService]
})

export class DocentAanmakenComponent {

    docentAanmaken: Docent = new Docent();
    docentId: Number;

    constructor(private docentaanmakenService: DocentaanmakenService) { }

    postDocent() {
        console.log(this.docentAanmaken);
        this.docentaanmakenService.postDocent(this.docentAanmaken).subscribe(docentId => {
            console.log("Docent aangemaakt, succes! - " + docentId);
            this.docentId = +docentId;
        });
    }
}