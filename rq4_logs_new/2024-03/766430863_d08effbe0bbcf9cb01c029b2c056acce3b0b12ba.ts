import { Inject, Injectable } from "@nestjs/common";
import axios, { AxiosInstance, AxiosResponse } from "axios";

import {
  INTERNAL_CAMPAY_CONFIG_OPTIONS,
  CampayInternalModuleConfigOptions
} from "./types";

@Injectable()
export class CampayHttpClientService {
  private $instance!: AxiosInstance;

  constructor(
    @Inject(INTERNAL_CAMPAY_CONFIG_OPTIONS)
    private readonly config: CampayInternalModuleConfigOptions
  ) {
    this.$instance = axios.create({
      baseURL: this.config.baseUrl
    });

    this.$instance.defaults.headers.common[
      "Authorization"
    ] = `Token ${this.config.apiKey}`;
    this.$instance.defaults.headers["Content-Type"] = "application/json";
    this.$instance.defaults.headers["Accept"] = "application/json";
  }

  async post<TBody = object, TResponse = void>({
    url,
    body,
    headers
  }: {
    url: string;
    body?: TBody;
    headers?: Record<string, any>;
  }): Promise<AxiosResponse<TResponse>> {
    const res = await this.$instance.post<
      unknown,
      AxiosResponse<TResponse>,
      TBody
    >(url, body, { headers });
    console.log("😇", res.data);
    return res;
  }

  async get<TParams = any, TResponse = void>({
    url,
    params,
    headers
  }: {
    url: string;
    params?: TParams;
    headers?: Record<string, any>;
  }): Promise<AxiosResponse<TResponse>> {
    const res = await this.$instance.get<
      unknown,
      AxiosResponse<TResponse>,
      TParams
    >(url, { headers, params });
    console.log("🤛", res.data);

    return res;
  }
}