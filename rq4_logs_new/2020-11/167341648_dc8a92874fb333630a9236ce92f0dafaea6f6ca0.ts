import { CutPacking } from "famifarm-typescript-models";
import { Api } from ".";

export class CutPackingsService {
  private token: string;
  private basePath: string;

  constructor(basePath: string, token: string) {
    this.token = token;
    this.basePath = basePath;
  }

  /**
   * @summary Creates a new cut packing
   * @param body packing to be created
   */
  public createPacking(body: CutPacking): Promise<CutPacking> {
    const uri = new URI(`${this.basePath}/v1/cutPackings`);
    const options = {
      method: "post",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${this.token}`
      },
      body: JSON.stringify(body)
    };

    return fetch(uri.toString(), options).then((response) => {
      return Api.handleResponse(response);
    });
  }

  /**
 * 
 * @summary Deletes a cut packing
 * @param packingId Packing id
*/
  public deletePacking(packingId: string): Promise<any> {
    const uri = new URI(`${this.basePath}/v1/cutPackings/${encodeURIComponent(String(packingId))}`);
    const options = {
      method: "delete",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${this.token}`
      }
    };

    return fetch(uri.toString(), options).then((response) => {
      return Api.handleResponse(response);
    });
  }


  /**
   * 
   * @summary Find a cut packing
   * @param packingId Packing id
  */
  public findPacking(packingId: string): Promise<CutPacking> {
    const uri = new URI(`${this.basePath}/v1/cutPackings/${encodeURIComponent(String(packingId))}`);
    const options = {
      method: "get",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${this.token}`
      }
    };

    return fetch(uri.toString(), options).then((response) => {
      return Api.handleResponse(response);
    });
  }


  /**
   * 
   * @summary List all cut packings
   * @param firstResult First index of results to be returned
   * @param maxResults How many items to return at one time
   * @param productId Filter results by product id
   * @param createdBefore Filter results by date
   * @param createdAfter Filter results by date
  */
  public listPackings(firstResult?: number, maxResults?: number, productId?: string, createdBefore?: string, createdAfter?: string): Promise<Array<CutPacking>> {
    const uri = new URI(`${this.basePath}/v1/cutPackings`);
    if (firstResult !== undefined && firstResult !== null) {
      uri.addQuery('firstResult', <any>firstResult);
    }
    if (maxResults !== undefined && maxResults !== null) {
      uri.addQuery('maxResults', <any>maxResults);
    }
    if (productId !== undefined && productId !== null) {
      uri.addQuery('productId', <any>productId);
    }
    if (createdBefore !== undefined && createdBefore !== null) {
      uri.addQuery('createdBefore', <any>createdBefore);
    }
    if (createdAfter !== undefined && createdAfter !== null) {
      uri.addQuery('createdAfter', <any>createdAfter);
    }

    const options = {
      method: "get",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${this.token}`
      }
    };

    return fetch(uri.toString(), options).then((response) => {
      return Api.handleResponse(response);
    });
  }


  /**
   * 
   * @summary Updates a cut packing
   * @param body Request payload
   * @param packingId Packing id
  */
  public updatePacking(body: CutPacking, packingId: string,): Promise<CutPacking> {
    const uri = new URI(`${this.basePath}/v1/cutPackings/${encodeURIComponent(String(packingId))}`);
    const options = {
      method: "put",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${this.token}`
      },
      body: JSON.stringify(body)
    };

    return fetch(uri.toString(), options).then((response) => {
      return Api.handleResponse(response);
    });
  }
}