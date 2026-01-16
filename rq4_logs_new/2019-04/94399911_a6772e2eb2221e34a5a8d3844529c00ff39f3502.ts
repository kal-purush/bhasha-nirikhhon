import { Application, Response, Request } from "express";
import * as Keycloak from "keycloak-connect";
import AbstractService from "../abstract-service";

export default abstract class DeliveryQualitiesService extends AbstractService {

  /**
   * Constructor
   * 
   * @param app Express app
   * @param keycloak Keycloak
   */
  constructor(app: Application, keycloak: Keycloak) {
    super();

    app.get(`/rest/v1${this.toPath('/deliveryQualities')}`, [ keycloak.protect() ], this.catchAsync(this.listDeliveryQualities.bind(this)));
  }


  /**
   * Lists delivery qualities
   * @summary Lists delivery qualities
   * Accepted parameters:
    * - (query) ItemGroupCategory itemGroupCategory - filter by item group category
  */
  public abstract listDeliveryQualities(req: Request, res: Response): Promise<void>;

}