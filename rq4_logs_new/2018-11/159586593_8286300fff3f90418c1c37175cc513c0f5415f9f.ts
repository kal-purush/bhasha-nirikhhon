import * as axios from 'axios';
import { EventEmitter } from 'events';
import * as _ from 'lodash';
// import Leads from './leads';

const debug = require('debug')('freshsales:client');

// define how long to wait API response before throwing a timeout error
const API_TIMEOUT = 15000;
const MAX_USE_PERCENT_DEFAULT = 90;

export interface FreshSalesOptions {
  domain: string;
  apiKey: string;
}

class Client extends EventEmitter {
  domain: string;
  baseUrl: string;
  apiKey: string;
  authHeader: { Authorization: string };
  apiCalls: number;
  axios: axios.AxiosInstance;
  constructor (options: FreshSalesOptions) {
    super();
    // this.qs = {};
    this.setBaseUrl(options);
    this.setApiKey(options);
    this.apiCalls = 0;
    this.on('apiCall', (params: any) => {
      debug('apiCall', _.pick(params, ['method', 'url']));
      this.apiCalls += 1;
    });
    this.axios = axios.default.create({ baseURL: this.baseUrl, headers: this.authHeader });
    // this.contacts = new Contact(this);
  }

  setBaseUrl(options: FreshSalesOptions) {
    if (!options.domain) {
      throw new Error('Please provide a domain');
    }
    this.domain = options.domain;
    this.baseUrl = `https://${this.domain}.freshsales.io/api/`;
  }

  setApiKey(options: FreshSalesOptions) {
    if (!options.apiKey) {
      throw new Error('Please provide an apiKey');
    }
    this.apiKey = options.apiKey;
    this.authHeader = { Authorization: `Token token=${this.apiKey}` };
  }

  async request (config: axios.AxiosRequestConfig) {
    const results = await this.axios(config);
    return results;
    // try {
    //   const results = await this.axios(config);
    //   console.log(results);
    //   return results;
    // } catch (err) {
    //   console.log(err);
    //   throw err;
    // }
    // to add: count API calls and handle response
  }

}

export default Client;