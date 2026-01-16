import { NextFunction, Request, Response } from 'express';
import { MiddlewareDefinition } from '@type';
import { getToken, getDefaultResponse } from '@helper';
import { getRepository } from 'typeorm';
import { Token, TokenScope } from '@entity';
import { validateToken } from '@middleware';

const metadataKey = 'routeMiddleware';

export const RequiredScope = (requiredScope: Partial<TokenScope>): MethodDecorator => {
    return (target: object, propertyKey: string): void => {
        const metadataValue = Reflect.getMetadata(metadataKey, target, propertyKey) as MiddlewareDefinition[] || [];

        metadataValue.push(validateToken);
        metadataValue.push(async (request: Request, response: Response, next: NextFunction) => {
            const token = getToken(request);
            const entity = await getRepository(Token).findOne({ token });

            if(!requiredScope.satisfiedBy(entity.scope)) {
                response.status(403).json(getDefaultResponse(403, request.path));
                return;
            }

            next();
        });

        Reflect.defineMetadata(metadataKey, metadataValue, target, propertyKey);
    };
};