/* eslint-disable no-unused-vars */
/* eslint-disable no-useless-constructor */

import { RootObject } from '../../models/api.model';

export interface PhotosApiRepoStructure {
  loadPhotos(): Promise<RootObject>;
}

const apiKey = '&api_key=B2ll490zBp9I0mmMbiA98Ji4wZyA9qNv52KS2qjP';

export class PhotosApiRepo {
  url: string;
  constructor() {
    this.url =
      'https://api.nasa.gov/mars-photos/api/v1/rovers/curiosity/photos?sol=3120&page=1';
  }

  async loadPhotos(camera?: string): Promise<RootObject> {
    const resp = await fetch(this.url + camera + apiKey);
    const data = (await resp.json()) as RootObject;
    return data;
  }
}