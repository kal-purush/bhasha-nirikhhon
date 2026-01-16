import Map = require('core-js/es6/map');
import { MetadataKeys } from '../constants/';
import { JsonArrayOptions } from './';

export function JsonArray(path?: string, options?: JsonArrayOptions) {
    return function (target: any, propertyKey: string) {
        if (!target || !propertyKey) {
            return;
        }

        let paths = [path || propertyKey];
        if (options && options.fallbacks) {
            paths = paths.concat(options.fallbacks);
        }

        target.pathMapping = target.pathMapping || new Map<string, string[]>();
        target.pathMapping.set(propertyKey, paths);

        const type = Reflect.getMetadata(MetadataKeys.TYPE, target, propertyKey);
        target.typeMapping = target.typeMapping || new Map<string, Function>();
        target.typeMapping.set(propertyKey, type);

        Reflect.defineMetadata(MetadataKeys.ARRAY_TYPE, options.type, target, propertyKey);
    };
}