import ApiConfig from './ApiConfig';

import axios, { AxiosInstance } from 'axios';

// @ts-ignore
import { version } from '../package.json';

import APIList from './api/APIList';
import APIListDevices from './api/APIListDevices';
import APIListUsers from './api/APIListUsers';

import APIResourceUser from './api/APIResourceUser';
import APIResourceDevice from './api/APIResourceDevice';
import APIResourceDeviceGroup from './api/APIResourceDeviceGroup';
import APIResourceUserSession from './api/APIResourceUserSession';

import APIAdminResource from './api/APIAdminResource';


if (globalThis.isNodeJs) {
  // Set User-Agent
  axios.defaults.headers.common['User-Agent'] = `Bitbar Cloud API Client for JavaScript v${version}`;
}

// Disable max content length
axios.defaults.maxContentLength = 1073741824; // 1GB


/**
 * API
 * Root for other API resources
 */
class API {

  // Main config
  private config: ApiConfig;

  // axios config
  private axiosConfig: ApiConfig;

  // axios instance
  public axios: AxiosInstance;


  constructor (config: ApiConfig) {
    if (config == null) {
      throw new Error('config cannot be empty');
    }

    this.config = config;
    this.axiosConfig = <ApiConfig> {};

    if (this.config.cloudUrl == null) {
      throw new TypeError('cloudUrl cannot be empty');
    } else if (typeof this.config.cloudUrl !== 'string') {
      throw new TypeError('cloudUrl must be a string');
    } else if (!/^https?:\/\/.{2,}/.test(this.config.cloudUrl)) {
      throw new Error("cloudUrl doesn't look like a URL");
    }

    // Validate and correct cloudUrl if needed
    this.axiosConfig.baseURL = this.config.cloudUrl.replace(/\/+$/, '') + '/api'

    // Check v2
    this.config.v2 = !!this.config.v2;
    if (this.config.v2) {
      this.axiosConfig.baseURL += '/v2';
    }

    // Check if apiKey is set
    if (this.config.apiKey) {
      if (typeof this.config.apiKey !== 'string') {
        throw new TypeError('apiKey must be a string');
      } else if (!/^[A-Za-z0-9]{32}$/.test(this.config.apiKey)) {
        throw new Error("apiKey is in the wrong format");
      }

      this.axiosConfig.auth = {
        username: <string> this.config.apiKey,
        password: ''
      };
    }

    // Create axios instance
    this.axios = axios.create(this.axiosConfig);
  }


  // --- Resources starts here --- //

  public userSession () {
    return new APIResourceUserSession(this);
  }

  // /user/{id}
  public user (id: number | 'me') {
    return new APIResourceUser(this, id);
  }

  // /users
  public users () {
    return new APIListUsers(this);
  }

  // /me
  public me () {
    return this.user('me');
  }

  // /admin
  public admin () {
    return new APIAdminResource(this);
  }

  // /devices
  public devices () {
    return new APIListDevices(this);
  }

  // /devices/{id}
  public device (id: number) {
    return new APIResourceDevice(this, id);
  }

  // /device-groups
  public deviceGroups () {
    return new APIList(this).push('device-groups');
  }

  // /device-groups/{id}
  public deviceGroup (id: number) {
    return new APIResourceDeviceGroup(this, id);
  }

  // /label-groups
  public labelGroups () {
    return new APIList(this).push('label-groups');
  }
}


export default API;