import { Component } from '@angular/core';

import { NavController, NavParams } from 'ionic-angular';
import { Storage } from '@ionic/storage';

import { LoggerService } from "../../common/log/logger.service";

import { SettingsService } from "./settings.service";

@Component({
  selector: 'page-home',
  templateUrl: 'settings.component.html'
})
export class SettingsComponent {

   /**
   * @description Title of component
   * @public
   */
    public title:string;

   /**
   * @description item list to be displayed
   * @public
   */
   public items: Array<any>;

  /**
   * @description Settings key. It is used to store and retrieve data from storage
   * @private
   */
   private SETTINGS_KEY: string = 'asset-tracker-store-settings';

  /**
   * @constructor 
   * @param navCtrl Navigation Controller
   * @param storage Storage Service provided by Ionic
   * @param logger Logger Service
   * @param settingsService Service of Settings module.
   */

  constructor(public navCtrl: NavController,
    private storage: Storage,
    private logger: LoggerService,
    private settingsService: SettingsService) {

    var context = this;

    storage.ready().then(() => {
      context.loadData();
    });
  }

  /**
   * @description Function to load data related to this component
   */
  loadData() {

    var context = this;

    context.storage.get(context.SETTINGS_KEY).then((storeData) => {

      //  True: when no value is stored in storage
      if (storeData === null || typeof storeData === 'undefined') {
         context.settingsService.getData().subscribe(data => {
           context.title = data.title;
           context.items = data.items;
           context.storage.set(context.SETTINGS_KEY, JSON.stringify(data));
         });
      } else {
        storeData = JSON.parse(storeData);
        context.title = storeData.title;
        context.items = storeData.items;
      }

    });
  }
}