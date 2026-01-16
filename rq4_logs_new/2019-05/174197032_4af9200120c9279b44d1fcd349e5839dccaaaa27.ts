/* tslint:disable */

declare var Object: any;
export interface SeatInterface {
  "row": number;
  "column": string;
  "classType": string;
  "id"?: any;
  "flightId"?: any;
}

export class Seat implements SeatInterface {
  "row": number;
  "column": string;
  "classType": string;
  "id": any;
  "flightId": any;
  constructor(data?: SeatInterface) {
    Object.assign(this, data);
  }
  /**
   * The name of the model represented by this $resource,
   * i.e. `Seat`.
   */
  public static getModelName() {
    return "Seat";
  }
  /**
  * @method factory
  * @author Jonathan Casarrubias
  * @license MIT
  * This method creates an instance of Seat for dynamic purposes.
  **/
  public static factory(data: SeatInterface): Seat{
    return new Seat(data);
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
      name: 'Seat',
      plural: 'Seats',
      path: 'Seats',
      idName: 'id',
      properties: {
        "row": {
          name: 'row',
          type: 'number'
        },
        "column": {
          name: 'column',
          type: 'string'
        },
        "classType": {
          name: 'classType',
          type: 'string'
        },
        "id": {
          name: 'id',
          type: 'any'
        },
        "flightId": {
          name: 'flightId',
          type: 'any'
        },
      },
      relations: {
      }
    }
  }
}