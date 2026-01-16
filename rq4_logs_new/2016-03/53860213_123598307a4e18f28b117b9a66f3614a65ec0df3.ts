/**
 * The export section is necessary to make the functions available to the
 * Swagger generated routes.
 */
export = {
  activateIntegration: AppController.activateIntegration,
  deactivateIntegration: AppController.deactivateIntegration
};

/**
 * The App module handling all client requests concerning the app itself.
 */
module AppController {

  /**
   * Activate the integration given in the payload.
   *
   * @param req
   * @param res
   */
  export function activateIntegration(req, res) {

  }

  /**
   * Deactivate the integration given in the payload.
   *
   * @param req
   * @param res
   */
  export function deactivateIntegration(req, res) {

  }
}