import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

import { MissionDetailInterface, ImageHelper } from '../mission-list/mission-list';
import { MissionListServiceProvider } from '../../providers/mission-list-service/mission-list-service';
import { LoadingServiceProvider } from '../../providers/loading-service/loading-service';

/**
 * @interface - Structure of the planet  object.
 * @param {string} name - Name of the planet.
 * @param {string} rotation_period - Rotation period of the planet.
 * @param {string} orbital_period - Orbital period of the planet.
 * @param {string} diameter - Diameter of the planet.
 * @param {string} climate - Climate of the planet.
 * @param {string} gravity - Gravity of the planet.
 * @param {string} terrain - Terrain of the planet.
 * @param {string} surface_water - Surface Water of the planet.
 * @param {string} population - Population of the planet.
 */
export interface PlanetDetailsInterface {
  name: string,
  rotation_period: string,
  orbital_period: string,
  diameter: string,
  climate: string,
  gravity: string,
  terrain: string,
  surface_water: string,
  population: string,
}

@Component({
  selector: 'page-mission-details',
  templateUrl: 'mission-details.html'
})
/**
 * Class Managing the Details page view.
 */
export class MissionDetailsPage {

  // Selected Mission Object
  selectedMission: MissionDetailInterface;

  // Planet details object
  planetDetails: PlanetDetailsInterface;

  // Id of the mission
  missionId: number;

  // Flag to check if the mission id is correct
  isError: boolean;

  /**
   * @constructor
   * @param {NavController} navCtrl
   * @param {NavParams} navParams
   * @param {MissionListServiceProvider} missionListService - Service to help retireve missions by id.
   * @param {LoadingServiceProvider} loadingService - Displays and hides loading screen.
   * @param {MissionListPage} listPage - List page component.
   */
  constructor(public navCtrl: NavController,
    public navParams: NavParams,
    public missionListService: MissionListServiceProvider,
    public loadingService: LoadingServiceProvider) {
    // If we navigated to this page, we will have an item available as a nav param.
    this.selectedMission = navParams.get('item');
    this.missionId = navParams.get('id');
    // Flag to determine if the id of the page is valid.
    this.isError = false;
  }


  /**
   * Invoked when the view is fully loaded.
   * Initiates an API request to load the mission by Id.
   * API request will be fired only if the user reaching this page directly by typing URL.
   * Once the mission is got, fires another request to get planet details.
   */
  ionViewDidLoad() {
    this.loadingService.showLoading();

    this.getMissionById(this.missionId)
      .then((item) => {
        this.selectedMission = item;
        let planetId = this.getPlanetIdFromMission(this.selectedMission);
        return this.getPlanetById(planetId);
      })
      .then(planetDetails => {
        this.planetDetails = planetDetails;
        return this.planetDetails;
      })
      .catch(err => {
        this.handleError(err);
      })
      .then(() => {
        this.loadingService.dismissLoading();
      });

  }


  /**
   * Gets the mission by id.
   * @param {number} id - Id to which the mission is to be retrieved.
   * @return {Promise} returns a promise which resolves to mission object.
   */
  getMissionById(id:number): Promise<any> {
    if(this.selectedMission) {
      return Promise.resolve(this.selectedMission);
    } else {
    return this.missionListService.getMissionById(id)
      .then(item => {
        item.image_big = ImageHelper.imagePath + ImageHelper.getRandomImageNumber().toString() + '-big.png';
        return item;
      })
      .catch(this.handleError);
    }
  }

  /**
   * Gets the planet by id.
   * @param {number} id - Id of the planet to be retrieved.
   * @return {Promise} returns a promise which resolves to planet object.
   */
  getPlanetById(id:number): Promise<any> {
    return this.missionListService.getPlanetById(id)
      .then(item => {
        return Promise.resolve(item);
      })
      .catch(this.handleError);
  }

  /**
   * Gets the planet by id from Mission object.
   * @param {MissionDetailInterface} item - Mission object.
   * @return {number} Returns the id of the planet.
   */
  getPlanetIdFromMission(item:MissionDetailInterface):number {
    let homeWorld = item.homeworld;
    if(!homeWorld) {
      return 1;
    } else {
      return Number(homeWorld.split('/').slice(-2, -1)[0]);
    }
  }

  /**
   * Handles errors from API request
   * @param {object} err - Error object.
   * @return {Promise} Returns a promise which resolves to err object;
   */
  private handleError(err): Promise<any> {
    this.isError = true;
    // Should be logged to the logging service ideally.
    return Promise.reject(err.message || err);
  }
}