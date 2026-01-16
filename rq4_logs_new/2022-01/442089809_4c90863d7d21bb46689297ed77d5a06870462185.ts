import { NetworkConstructor } from "./../network/http.js";
import { ClientNetwork } from "../network/http.js";
import { baseURL } from "../app.js";

export type imageUploadFormat = {
  title: string;
  description: string;
  saleSite: string;
  media?: string;
  options: string[];
};

export interface postService {
  get(writer?: string): Promise<any>;
  imagePost(data: any): Promise<any>;
}

export type postServiceConstructor = {
  new (network: NetworkConstructor): postService;
};

export class mediaFetchService implements postService {
  private network: ClientNetwork;
  constructor(network: NetworkConstructor) {
    this.network = new network(baseURL);
  }

  async get(writer?: string): Promise<any> {
    const url = writer ? `/slime/${writer}` : `/slime`;
    return this.network.requestWithJson(url, {
      method: "GET",
    });
  }

  async imagePost(data: any): Promise<any> {
    const body = new FormData();
    for (const key in data) {
      body.append(key, data[key]);
    }
    body.append("media", "image");
    return this.network.requestWithFile("/slime/image", {
      method: "POST",
      body,
    });
  }
}