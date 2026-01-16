import {Component} from '@angular/core';
import {NavController} from 'ionic-angular';
import {PandasTagliaPage} from '../pandas-taglia-page/pandas-taglia-page';
import {PandasService} from '../../services';

@Component({
    templateUrl: 'build/pages/pandas-ora-page/pandas-ora-page.html'
})
export class PandasOraPage {
    private ora: string = '13:00';
    constructor(private _navController: NavController, private pandasService: PandasService) {
    }

    ionViewWillEnter() {
    }

    goToNextPage(){
        this.pandasService.setOra(this.ora);
        this._navController.push(PandasTagliaPage)
    }
}