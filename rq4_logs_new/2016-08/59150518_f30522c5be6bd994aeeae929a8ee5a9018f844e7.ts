import {provide, Injector} from '@angular/core';
import {describe, expect, it, xit, inject, async, fakeAsync, beforeEachProviders} from '@angular/core/testing';

import {Observable} from 'rxjs/Rx';

import {MealProvider} from './meal-provider.model.ts';
import {MealProviderService} from './meal-provider.service.ts';

import {XpathService} from '../xpath/xpath.service';

import {MapService} from '../map/map.service';

describe('Test MealProvider', () => {

  beforeEachProviders(() => {
      return [
          // I don't want to mock everything till the end of the world
          provide(MapService, {useValue: new MapService(null)}),
          MealProviderService,
          XpathService
      ];
    });

    it(' should prepare mock data if no cached providers available', async(inject([MealProviderService], (testService: MealProviderService) => {
        let mealProviders = [];
        spyOn(testService, 'getCachedMealProviders').and.callFake(() => {
          return [];
        });
        spyOn(testService, 'cacheMealProviders').and.callFake((providers) => {
          return mealProviders = providers;
        });
        testService.init();
        expect(mealProviders.length).toBe(2);
        expect(mealProviders[0].name).toBe('Bonnie');
    })));

    it(' don\'t prepare mock data if there are cached provider(s) available', async(inject([MealProviderService], (testService: MealProviderService) => {
        let mealProviders = [new MealProvider('test', null, {}, null, [], null, null)];
        spyOn(testService, 'getCachedMealProviders').and.callFake(() => {
          return [new MealProvider('test', null, {}, null, [], null, null)];
        });
        spyOn(testService, 'cacheMealProviders').and.callFake((providers) => {
          return mealProviders = providers;
        });
        testService.init();
        expect(mealProviders.length).toBe(1);
        expect(mealProviders[0].name).toBe('test');
    })));

    it(' adds new meal provider', async(inject([MealProviderService], (testService: MealProviderService) => {
        let existingProvider = new MealProvider('test', null, {}, null, [], null, null);
        let newProvider = new MealProvider('new', null, {}, null, [], null, null);
        spyOn(testService, 'getCachedMealProviders').and.callFake(() => {
          return [existingProvider];
        });
        let setItemSpy = spyOn(localStorage, 'setItem');
        testService.addMealProvider(newProvider);
        expect(setItemSpy).toHaveBeenCalledWith('mealProviders', JSON.stringify([existingProvider,newProvider]));
    })));

});