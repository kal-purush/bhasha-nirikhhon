import { ChangeDetectionStrategy, Component, EventEmitter, Input, OnInit, Output, ViewChild, Injectable } from '@angular/core';

import {CORE_DIRECTIVES} from '@angular/common';
import {ROUTER_DIRECTIVES} from '@angular/router';
import { TOOLTIP_DIRECTIVES } from 'ng2-tooltip';

import {TranslateService, TranslatePipe} from 'ng2-translate';

import { Fleet } from "../model/fleet";
import { FleetService } from '../service/fleet.service';

@Component({
    selector: 'fleet',
    pipes: [TranslatePipe],
    templateUrl: 'app/fleet/fleet.html',
    directives: [ TOOLTIP_DIRECTIVES, ROUTER_DIRECTIVES ],
    providers: [ FleetService ]

})
    
export class fleetList {
        
    constructor(
        private fleetService: FleetService,  
        public translate: TranslateService) {}
}