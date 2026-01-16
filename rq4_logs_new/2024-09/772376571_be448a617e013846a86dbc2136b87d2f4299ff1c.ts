import { middlewareRunner } from "../lib/Middleware";
import { NonEmptyArray } from "../lib/Types/HelperTypes";
import { TiFAPIClient, TiFAPISchema } from "./TiFAPISchema";
import { APIMiddleware, APISchema } from "./TransportTypes";

export const APIClientCreator = <T>(endpointSchema: APISchema, ...apiMiddleware: NonEmptyArray<APIMiddleware<T>>) => 
  Object.entries(endpointSchema).reduce((apiClient: any, [endpointName, endpointSchema]) =>
    {
      apiClient[endpointName] = ({...input}: any = {}) => middlewareRunner(...apiMiddleware)({...input, endpointName, endpointSchema})
      return apiClient
    }
  , {})

export const TiFAPIClientCreator = <T>(...apiMiddleware: NonEmptyArray<APIMiddleware<T>>): TiFAPIClient<T> => 
  APIClientCreator(TiFAPISchema, ...apiMiddleware)