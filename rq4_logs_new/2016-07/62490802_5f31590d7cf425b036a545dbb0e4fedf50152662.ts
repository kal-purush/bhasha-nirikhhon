import 'fetch';
import {autoinject}       from 'aurelia-framework';
import {HttpClient}       from 'aurelia-fetch-client';
import {ApplicationState} from './application-state';
import {DialogService}    from 'aurelia-dialog';
import {GithubCreds}      from './github-creds';

@autoinject()
export class NPMAPI {
  npmAPIUrl = 'https://registry.npmjs.org/';
  client: HttpClient;

  constructor() {
    this.client = new HttpClient();
  }

  async getLatest(repository) {
    return this.client.fetch(`${this.npmAPIUrl}-/package/${repository}/dist-tags`)
    .then(response => response.json())
    .then(data => data.latest);
  }
}