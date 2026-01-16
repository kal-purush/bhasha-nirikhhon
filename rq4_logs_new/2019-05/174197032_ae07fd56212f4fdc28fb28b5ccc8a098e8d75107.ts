/* tslint:disable */

declare var Object: any;
export interface ReservationOfferInterface {
  "roomReservationId": number;
  "specialOfferId": string;
  "id"?: any;
}

export class ReservationOffer implements ReservationOfferInterface {
  "roomReservationId": number;
  "specialOfferId": string;
  "id": any;
  constructor(data?: ReservationOfferInterface) {
    Object.assign(this, data);
  }
  /**
   * The name of the model represented by this $resource,
   * i.e. `ReservationOffer`.
   */
  public static getModelName() {
    return "ReservationOffer";
  }
  /**
  * @method factory
  * @author Jonathan Casarrubias
  * @license MIT
  * This method creates an instance of ReservationOffer for dynamic purposes.
  **/
  public static factory(data: ReservationOfferInterface): ReservationOffer{
    return new ReservationOffer(data);
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
      name: 'ReservationOffer',
      plural: 'ReservationOffers',
      path: 'ReservationOffers',
      idName: 'id',
      properties: {
        "roomReservationId": {
          name: 'roomReservationId',
          type: 'number'
        },
        "specialOfferId": {
          name: 'specialOfferId',
          type: 'string'
        },
        "id": {
          name: 'id',
          type: 'any'
        },
      },
      relations: {
      }
    }
  }
}