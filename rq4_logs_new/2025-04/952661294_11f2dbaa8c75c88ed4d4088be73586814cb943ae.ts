import { RequestAuth } from "./request.auth.entity";
import { RequestBody } from "./request.body.entity";
import { RequestDocs } from "./request.docs.entity";
import { RequestHeaders } from "./request.headers.entity";
import { RequestParams } from "./request.params.entity";
import { RequestResponse } from "./request.response.entity";


export type RequestEntity = {
  id: string;
  title?: string;
  url?: string;
  method?: "GET" | "POST" | "PUT" | "DELETE" | "PATCH";
  body?: RequestBody;
  docs?: RequestDocs;
  params?: RequestParams;
  auth?: RequestAuth;
  headers?: RequestHeaders[];
  response?: RequestResponse;
};