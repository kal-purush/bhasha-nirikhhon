import { Component } from '@angular/core';
import { AppUpdateProvider } from '../../providers/app-update/app-update';
import { AppSetting } from '../../providers/config/app-setting/app-setting';
import { UpdateComponent } from '../update/update';
import { AlertController } from 'ionic-angular';
import { Storage } from '@ionic/storage';

/**
 * Generated class for the UpdateAutoComponent component.
 *
 * See https://angular.io/docs/ts/latest/api/core/index/ComponentMetadata-class.html
 * for more info on Angular Components.
 */
@Component({
  selector: 'update-auto',
  templateUrl: 'update-auto.html'
})
export class UpdateAutoComponent extends UpdateComponent {
  constructor(private appUpdateExtend: AppUpdateProvider,
    private appConfigExtend: AppSetting,
    private alertCtrlExtend: AlertController,
    private storageExtend: Storage) {
    super(appUpdateExtend, appConfigExtend, alertCtrlExtend, storageExtend);
  }

}