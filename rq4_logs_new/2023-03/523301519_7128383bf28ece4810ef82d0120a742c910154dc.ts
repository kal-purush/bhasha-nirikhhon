import * as express from "express";
import IApplicationResources from "../../common/IApplicationResources.interface";
import IRouter from "../../common/IRouter.interface";
import AuthMiddleware from "../../middleware/AuthMiddleware";
import FlightLegController from "./FlightLegController.controller";

class FlightLegRouter implements IRouter {
  public setupRoutes(
    application: express.Application,
    resources: IApplicationResources
  ) {
    const flightLegController: FlightLegController = new FlightLegController(
      resources.services
    );

    application.get(
      "/api/flight-leg",
      AuthMiddleware.getVerifier("administrator", "user"),
      flightLegController.getAll.bind(flightLegController)
    );

    application.get(
      "/api/flight-leg/:flid",
      AuthMiddleware.getVerifier("administrator", "user"),
      flightLegController.getById.bind(flightLegController)
    );

    application.post(
      "/api/flight-leg",
      AuthMiddleware.getVerifier("administrator"),
      flightLegController.add.bind(flightLegController)
    );

    application.put(
      "/api/flight-leg/:flid",
      AuthMiddleware.getVerifier("administrator"),
      flightLegController.editById.bind(flightLegController)
    );
  }
}

export default FlightLegRouter;