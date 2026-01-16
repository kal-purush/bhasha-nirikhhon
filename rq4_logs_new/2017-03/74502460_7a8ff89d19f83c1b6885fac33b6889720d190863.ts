import { NO_ERRORS_SCHEMA } from '@angular/core';
import { inject, TestBed, ComponentFixture } from '@angular/core/testing';
import { EffectsTestingModule, EffectsRunner } from '@ngrx/effects/testing';
import { MeteoEffects } from './meteo.effects';

import { HttpModule } from '@angular/http';

import { Action, Store, StoreModule } from '@ngrx/store';
import { reducer } from '../reducers';
import { MeteoService } from '../../meteo/meteo.service';

import * as geo from '../actions/geo.actions';
import * as meteo from '../actions/meteo.actions';

import * as fromRoot from '../reducers';

import CurrentPosition from '../../models/position';

describe('MeteoEffect', () => {
    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [
                HttpModule,
                EffectsTestingModule,
                StoreModule.provideStore(reducer)
            ],
            providers: [
                MeteoEffects,
                MeteoService
            ],
            schemas: [NO_ERRORS_SCHEMA]
        })
    });

    let runner: EffectsRunner;
    let meteoEffects: MeteoEffects;

    beforeEach(inject([EffectsRunner, MeteoEffects], (_runner, _meteoEffects) => {
        runner = _runner;
        meteoEffects = _meteoEffects;

        spyOn(navigator.geolocation, 'getCurrentPosition').and.callFake(function () {
            const position: CurrentPosition = {
                coords: { latitude: 53.9333343, longitude: 27.65 }
            };
            arguments[0](position);
        });
    }));

    it('should return a GET_POSITION_SUCCESS action after positioned', () => {
        runner.queue({ type: geo.ActionTypes.GET_POSITION });

        meteoEffects.getPosition$.subscribe((result: Action) => {
            expect(result.type).toEqual(geo.ActionTypes.GET_POSITION_SUCCESS);
        });
    });

    it('should return a LOAD_FAIL action after LOAD action', inject([Store], (store: Store<fromRoot.State>) => {
        runner.queue({ type: meteo.ActionTypes.LOAD });

        meteoEffects.getAllCities$.subscribe((result: Action) => {
            expect(result.type).toEqual(meteo.ActionTypes.LOAD_FAIL);
        });
    }));

    it('should return a LOAD_FAIL action after FILTER action', () => {
        runner.queue({ type: meteo.ActionTypes.FILTER });

        meteoEffects.setFilters$.subscribe((result: Action) => {
            expect(result.type).toEqual(meteo.ActionTypes.LOAD_FAIL);
        });
    });
});