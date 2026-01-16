/* tslint:disable */

declare var Object: any;
export interface QuickFlightReservationInterface {
  "discount": number;
  "seatReservationId": string;
  "id"?: number;
}

export class QuickFlightReservation implements QuickFlightReservationInterface {
  "discount": number;
  "seatReservationId": string;
  "id": number;
  constructor(data?: QuickFlightReservationInterface) {
    Object.assign(this, data);
  }
  /**
   * The name of the model represented by this $resource,
   * i.e. `QuickFlightReservation`.
   */
  public static getModelName() {
    return "QuickFlightReservation";
  }
  /**
  * @method factory
  * @author Jonathan Casarrubias
  * @license MIT
  * This method creates an instance of QuickFlightReservation for dynamic purposes.
  **/
  public static factory(data: QuickFlightReservationInterface): QuickFlightReservation{
    return new QuickFlightReservation(data);
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
      name: 'QuickFlightReservation',
      plural: 'QuickFlightReservations',
      path: 'QuickFlightReservations',
      idName: 'id',
      properties: {
        "discount": {
          name: 'discount',
          type: 'number'
        },
        "seatReservationId": {
          name: 'seatReservationId',
          type: 'string'
        },
        "id": {
          name: 'id',
          type: 'number'
        },
      },
      relations: {
      }
    }
  }
}