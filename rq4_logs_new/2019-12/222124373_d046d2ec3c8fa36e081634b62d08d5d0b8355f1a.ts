import { NextFunction, Request, Response } from 'express';
import { MiddlewareDefinition } from '@type';
import { DefaultMessages, ValidatorMethod } from '@helper';

const metadataKey = 'routeMiddleware';

export const ValidateArgs = (dataSource: 'body' | 'params' | 'query', toValidate: { [k: string]: ValidatorMethod | ValidatorMethod[]}): MethodDecorator => {
    return (target: object, propertyKey: string): void => {
        const metadataValue = Reflect.getMetadata(metadataKey, target, propertyKey) as MiddlewareDefinition[] || [];

        metadataValue.push(async (request: Request, response: Response, next: NextFunction) => {
            const args = request[dataSource];

            const errors = [];
            Object.keys(args).forEach((argKey) => {
                const key = toValidate[argKey];
                if (!key) {
                    return;
                }

                if (!toValidate[argKey]) {
                    return;
                }

                if (typeof toValidate[argKey] === 'function') {
                    const validator = toValidate[argKey] as ValidatorMethod;
                    const validatorResponse = validator(args[argKey]);
                    if (typeof validatorResponse === 'boolean' && !validatorResponse) {
                        errors.push({
                            path: `.${dataSource}.${argKey}`,
                            message: `Invalid value suplied for ${argKey}.`,
                        });
                    }

                    if (typeof validatorResponse === 'object' && !validatorResponse) {
                        (validatorResponse as string[]).forEach((validateError) => {
                            errors.push({
                                path: `.${dataSource}.${argKey}`,
                                message: validateError
                            });
                        });
                    }
                } else {
                    for (let i = 0; i < (toValidate[argKey] as ValidatorMethod[]).length; i++) {
                        const validator = (toValidate[argKey] as ValidatorMethod[])[i] as ValidatorMethod;
                        const validatorResponse = validator(args[argKey]);

                        if (typeof validatorResponse === 'boolean' && !validatorResponse) {
                            errors.push({
                                path: `.${dataSource}.${argKey}`,
                                message: `Invalid value suplied for ${argKey}.`,
                            });
                        }

                        if (typeof validatorResponse === 'object' && !validatorResponse) {
                            (validatorResponse as string[]).forEach((validateError) => {
                                errors.push({
                                    path: `.${dataSource}.${argKey}`,
                                    message: validateError
                                });
                            });
                        }
                    }
                }
            });

            if (errors.length > 0) {
                return response.status(400).json({
                    message: DefaultMessages[400],
                    errors
                });
            }

            next();
        });

        Reflect.defineMetadata(metadataKey, metadataValue, target, propertyKey);
    };
};