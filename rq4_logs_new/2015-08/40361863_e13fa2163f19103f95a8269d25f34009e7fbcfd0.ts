/// <reference path="../../node_modules/reflect-metadata/reflect-metadata.d.ts"/>

require('reflect-metadata');

export function Inject(className: string) {
  return function (target: any, propertyKey: string) {
    var services = new Array<InjectData>();
    if (Reflect.hasMetadata(MetaData.INJECTS, target)) {
      services = Reflect.getMetadata(MetaData.INJECTS, target);
    } else {
      Reflect.defineMetadata(MetaData.INJECTS, services, target);
    }
    services.push({
      name:  propertyKey,
      className: className
    });
  }
}

export enum MetaData {
  INJECTS
}

export interface InjectData {
  name:  string;
  className: string;
}