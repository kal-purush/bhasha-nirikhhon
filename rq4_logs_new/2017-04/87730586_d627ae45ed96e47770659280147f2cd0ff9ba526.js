import angular from 'angular';
import 'angular-mocks';
import riaAirportSearchInput from './airport-search-input.component';
import reduxStore from '../../_redux-store';

describe('airport search input component', () => {
  beforeEach(() => {
    angular.mock.module(riaAirportSearchInput);
    angular.mock.module(reduxStore);
  });

  it('should compile as origin input', angular.mock.inject(($rootScope, $componentController, $ngRedux, riaAirportActions, riaFlightDetailsActions) => {
    const model = {};
    const scope = $rootScope.$new();
    const controller = $componentController('riaAirportSearchInput',
    {$ngRedux, $scope: scope, riaAirportActions, riaFlightDetailsActions},
      {type: 'origin',
        placeHolder: 'test placeholder',
        ngDisabled: false,
        model,
        items: [{}],
        notFoundMessage: 'not found test'
      });
    controller.$onInit();
    expect(controller.type).toBe('origin');
  }));

  it('should set the correct notFoundMessage when it is destination input', angular.mock.inject(($rootScope, $componentController, $ngRedux, riaAirportActions, riaFlightDetailsActions) => {
    const model = {};
    const scope = $rootScope.$new();
    const controller = $componentController('riaAirportSearchInput',
    {$ngRedux, $scope: scope, riaAirportActions, riaFlightDetailsActions},
      {type: 'destination',
        placeHolder: 'test placeholder',
        ngDisabled: false,
        model,
        items: [{}]
      });

    let data = {items: {currentValue: [{}, {}, {}]}};
    controller.props.airports = {selectedOrigin: {}};
    controller.searchText = 'test';
    controller.$onChanges(data);
    expect(controller.notFoundMessage).toBe('No such airport was found in our database');

    controller.notFoundMessage = undefined;
    data = {items: {currentValue: []}};
    controller.$onChanges(data);
    expect(controller.notFoundMessage).toBe('Sorry, no connected airports were found for this origin');
  }));

  it('should compile as destination input', angular.mock.inject(($rootScope, $componentController, $ngRedux, riaAirportActions, riaFlightDetailsActions) => {
    const model = {};
    const scope = $rootScope.$new();
    const controller = $componentController('riaAirportSearchInput',
    {$ngRedux, $scope: scope, riaAirportActions, riaFlightDetailsActions},
      {type: 'destination',
        placeHolder: 'test placeholder',
        ngDisabled: false,
        model,
        items: [{}],
        notFoundMessage: 'not found test'
      });
    controller.$onInit();
    expect(controller.type).toBe('destination');
  }));

  it('should throw error if type is not "origin" or "destination"', angular.mock.inject(($rootScope, $componentController, $ngRedux, riaAirportActions, riaFlightDetailsActions) => {
    const model = {};
    const scope = $rootScope.$new();
    const controller = $componentController('riaAirportSearchInput',
    {$ngRedux, $scope: scope, riaAirportActions, riaFlightDetailsActions},
      {type: 'something else',
        placeHolder: 'test placeholder',
        ngDisabled: false,
        model,
        items: [{}],
        notFoundMessage: 'not found test'
      });
    expect(() => controller.$onInit()).toThrow(new Error('Airport search input supports only "origin" and "destination" as type'));
  }));

  it('should search as destination input', angular.mock.inject(($rootScope, $componentController, $ngRedux, riaAirportActions) => {
    const model = {};
    const scope = $rootScope.$new();
    const controller = $componentController('riaAirportSearchInput',
    {$ngRedux, $scope: scope, riaAirportActions},
      {type: 'destination',
        placeHolder: 'test placeholder',
        ngDisabled: false,
        model,
        items: [{}],
        notFoundMessage: 'not found test'
      });
    const spyFilterDestinationAirport = spyOn(controller.props, 'filterDestinationAirport').and.callThrough();
    const spyFilterOriginAirport = spyOn(controller.props, 'filterOriginAirport').and.callThrough();
    controller.searchTextChange('test');
    expect(spyFilterDestinationAirport).toHaveBeenCalled();
    expect(spyFilterOriginAirport).not.toHaveBeenCalled();
  }));

  it('should search as origin input', angular.mock.inject(($rootScope, $componentController, $ngRedux, riaAirportActions) => {
    const model = {};
    const scope = $rootScope.$new();
    const controller = $componentController('riaAirportSearchInput',
    {$ngRedux, $scope: scope, riaAirportActions},
      {type: 'origin',
        placeHolder: 'test placeholder',
        ngDisabled: false,
        model,
        items: [{}],
        notFoundMessage: 'not found test'
      });
    const spyFilterDestinationAirport = spyOn(controller.props, 'filterDestinationAirport').and.callThrough();
    const spyFilterOriginAirport = spyOn(controller.props, 'filterOriginAirport').and.callThrough();
    controller.searchTextChange('test');
    expect(spyFilterDestinationAirport).not.toHaveBeenCalled();
    expect(spyFilterOriginAirport).toHaveBeenCalled();
  }));

  it('should set origin airport if type is origin and fire get connected airports', angular.mock.inject(($rootScope, $componentController, $ngRedux, riaAirportActions) => {
    const model = {};
    const scope = $rootScope.$new();
    const controller = $componentController('riaAirportSearchInput',
    {$ngRedux, $scope: scope, riaAirportActions},
      {type: 'origin',
        placeHolder: 'test placeholder',
        ngDisabled: false,
        model,
        items: [{}],
        notFoundMessage: 'not found test'
      });
    const spySetOriginAirport = spyOn(controller.props, 'setOriginAirport').and.callThrough();
    const spySetDestinationAirport = spyOn(controller.props, 'setDestinationAirport').and.callThrough();
    controller.setSelectedAirport({test: 'airport'});
    expect(spySetOriginAirport).toHaveBeenCalled();
    expect(spySetDestinationAirport).not.toHaveBeenCalled();
  }));

  it('should set destination airport if type is destination', angular.mock.inject(($rootScope, $componentController, $ngRedux, riaAirportActions) => {
    const model = {};
    const scope = $rootScope.$new();
    const controller = $componentController('riaAirportSearchInput',
    {$ngRedux, $scope: scope, riaAirportActions},
      {type: 'destination',
        placeHolder: 'test placeholder',
        ngDisabled: false,
        model,
        items: [{}],
        notFoundMessage: 'not found test'
      });
    const spySetOriginAirport = spyOn(controller.props, 'setOriginAirport').and.callThrough();
    const spySetDestinationAirport = spyOn(controller.props, 'setDestinationAirport').and.callThrough();
    controller.setSelectedAirport({test: 'airport'});
    expect(spySetOriginAirport).not.toHaveBeenCalled();
    expect(spySetDestinationAirport).toHaveBeenCalled();
  }));
});