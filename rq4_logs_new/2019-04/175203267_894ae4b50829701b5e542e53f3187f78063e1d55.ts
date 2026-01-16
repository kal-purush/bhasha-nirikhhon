import { config } from './../config';
import axios from "axios";
import { UpdateClientRoute } from '../routes/update-client';

/**
 * Handles subscribing to the Client Information Subsystem
 */
export class CISSubscriber {
  private static readonly cisUrl =
    "https://cos301-ocean-cis-api.herokuapp.com/subscribe";

  public static async subscribe() {
    const res = await CISSubscriber.subscribeRequest();
    console.log(res.data);
    setInterval(async () => {
      const res = await CISSubscriber.subscribeRequest();
      console.log(res.data);
    }, 10 * 60 * 1000);
  }

  private static subscribeRequest() {
    return axios.post(CISSubscriber.cisUrl, {
      subsystem: "CRDS",
      url: `${config.deploymentUrl}${new UpdateClientRoute().getEndpoint()}`
    });
  }
}