import { Directus, Auth } from '@directus/sdk';

// const directus = new Directus('http://localhost:8055');

export class DirectusInstance {
  instance: Directus<any, Auth>;

  constructor(url: string, init: any = {}) {
    this.instance = new Directus(url);
  }

  async getMany(collectionName: string, query: any): Promise<any> {
    const response = await this.instance.items(collectionName).readByQuery(query);
    return response;
  }

  async getOne(collectionName: string, id: string): Promise<any> {
    const response = await this.instance.items(collectionName).readOne(id);
    return response;
  }

  async post(itemName: string, createPayload: any): Promise<any> {
    const response = await this.instance.items(itemName).createOne(createPayload);
    if (response === null || response === undefined) {
      return undefined;
    } else {
      return response;
    }
  }

  async patch(itemName: string, itemId: string, requestPayload: any): Promise<any> {
    const response = await this.instance.items(itemName).updateOne(itemId, requestPayload);
    if (response === null || response === undefined) {
      return undefined;
    } else {
      return response;
    }
  }  
}

const url = process.env.REACT_APP_DIRECTUS_URL as string;
const init = {
  staticToken: process.env.REACT_APP_DIRECTUS_TOKEN
}

// console.log('url', url, init);

export const directusEnv = new DirectusInstance(url)