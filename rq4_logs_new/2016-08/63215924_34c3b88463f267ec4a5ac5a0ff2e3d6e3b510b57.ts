import { OpaqueToken } from '@angular/core';


interface AppSettings {
    apiGatewayBasePath: string;
    apiStrategyBasePath: string;
    productLibFolder: string;
    isFirmStructureLoadOnDemand: boolean;
    cacheExpiryInMinutes: number;
}

const constAppSettings: AppSettings = {
    apiGatewayBasePath: 'http://localhost:10020',
    apiStrategyBasePath: 'http://localhost:10010',
    productLibFolder: 'node_modules',
    isFirmStructureLoadOnDemand: true,
    cacheExpiryInMinutes: 1,
};

let appSettings = new OpaqueToken('app.settings');

export {AppSettings, constAppSettings, appSettings }