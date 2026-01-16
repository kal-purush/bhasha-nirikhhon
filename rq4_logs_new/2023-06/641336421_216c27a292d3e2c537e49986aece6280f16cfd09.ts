import autobind from 'autobind-decorator';
import { AxiosInstance, HttpStatusCode } from 'axios';

export enum flagResourceType {
  PARTY = 'PARTY',
  CASE = 'CASE',
}

export type QueryParams = {
  [key: string]: string | number;
};

export class FlagType {
  public name = '';
  public name_cy = '';
  public hearingRelevant = false;
  public flagComment = false;
  public defaultStatus: string | undefined;
  public externallyAvailable = false;
  public flagCode = '';
  public isParent = 'false';
  // Note: property is deliberately spelt "Path" and not "path" because the Reference Data Common API returns the former
  public Path: string[] = [];
  public childFlags: FlagType[] = [];
  public listOfValuesLength = 0;
  public listOfValues: { key: string; value: string }[] = [];
}

@autobind
export class RefData {
  constructor(private client: AxiosInstance) {}

  public async getFlags(serviceId: string, flagType?: flagResourceType, welsh?: boolean): Promise<FlagType> {
    const path = `/flag/${serviceId}`;

    const queryParams: QueryParams = {};

    if (typeof flagType !== 'undefined') {
      Object.assign(queryParams, { 'flag-type': flagType });
    }

    if (typeof welsh !== 'undefined') {
      const isWelsh = welsh ? 'Y' : 'N';
      Object.assign(queryParams, { 'welsh-required': isWelsh });
    }

    let queryString = '';
    if (Object.keys(queryParams).length !== 0) {
      queryString = Object.keys(queryParams)
        .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(queryParams[key])}`)
        .join('&');
    }

    const response = await this.client.get(`${path}?${queryString}`);

    if (
      response.status !== HttpStatusCode.Ok ||
      !response.data ||
      !response.data.flags ||
      !response.data.flags.length ||
      !response.data.flags[0].FlagDetails ||
      !response.data.flags[0].FlagDetails.length
    ) {
      // Note: Reference Data Common API appears to respond with a 404 error rather than send an empty response,
      // so this may be redundant
      throw new Error('No flag types could be retrieved');
    }

    return response.data.flags[0].FlagDetails;
  }
}