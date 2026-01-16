import { NextFunction, Request, Response } from 'express';
import { MiddlewareDefinition } from './../types/middlewareDefinition';

const metadataKey: string = 'routeMiddleware';

export const AdminScope = (): MethodDecorator => {
  return (target: any, propertyKey: string): void => {
    const metadataValue = Reflect.getMetadata(metadataKey, target, propertyKey) as MiddlewareDefinition[] || [];

    metadataValue.push((request: Request, response: Response, next: NextFunction) => {
      console.log('a');
      next();
    });

    Reflect.defineMetadata(metadataKey, metadataValue, target, propertyKey);
  };
};