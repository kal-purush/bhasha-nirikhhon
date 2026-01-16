/* tslint:disable */
import {
  Car,
  Myuser
} from '../index';

declare var Object: any;
export interface CarReservationInterface {
  "startDate": Date;
  "endDate": Date;
  "price": number;
  "id"?: number;
  "carsId"?: any;
  "myuserId"?: any;
  cars?: Car;
  myuser?: Myuser;
}

export class CarReservation implements CarReservationInterface {
  "startDate": Date;
  "endDate": Date;
  "price": number;
  "id": number;
  "carsId": any;
  "myuserId": any;
  cars: Car;
  myuser: Myuser;
  constructor(data?: CarReservationInterface) {
    Object.assign(this, data);
  }
  /**
   * The name of the model represented by this $resource,
   * i.e. `CarReservation`.
   */
  public static getModelName() {
    return "CarReservation";
  }
  /**
  * @method factory
  * @author Jonathan Casarrubias
  * @license MIT
  * This method creates an instance of CarReservation for dynamic purposes.
  **/
  public static factory(data: CarReservationInterface): CarReservation{
    return new CarReservation(data);
  }
  /**
  * @method getModelDefinition
  * @author Julien Ledun
  * @license MIT
  * This method returns an object that represents some of the model
  * definitions.
  **/
  public static getModelDefinition() {
    return {
      name: 'CarReservation',
      plural: 'CarReservations',
      path: 'CarReservations',
      idName: 'id',
      properties: {
        "startDate": {
          name: 'startDate',
          type: 'Date'
        },
        "endDate": {
          name: 'endDate',
          type: 'Date'
        },
        "price": {
          name: 'price',
          type: 'number'
        },
        "id": {
          name: 'id',
          type: 'number'
        },
        "carsId": {
          name: 'carsId',
          type: 'any'
        },
        "myuserId": {
          name: 'myuserId',
          type: 'any'
        },
      },
      relations: {
        cars: {
          name: 'cars',
          type: 'Car',
          model: 'Car',
          relationType: 'belongsTo',
                  keyFrom: 'carsId',
          keyTo: 'id'
        },
        myuser: {
          name: 'myuser',
          type: 'Myuser',
          model: 'Myuser',
          relationType: 'belongsTo',
                  keyFrom: 'myuserId',
          keyTo: 'id'
        },
      }
    }
  }
}