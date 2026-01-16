/**
 * Created by jwshin on 1/12/2015.
 */
///<reference path="../../typings/tsd.d.ts" />
///<reference path="esriService.ts" />

import esri = require("esri");

module Application.Services {
    export interface IMapService {

    }

    export class MapService {
        log:ng.ILogService;
        map;
        toolbar;
        constructor($log:ng.ILogService) {
            this.log = $log;
        }
        addLayer(layer) {
            try {
                this.map.addLayer(layer);
            } catch(e) {
                throw new Error(e);
            }
        }
        createMap(id: string, options:esri.MapOptions) {

        }
    }
}