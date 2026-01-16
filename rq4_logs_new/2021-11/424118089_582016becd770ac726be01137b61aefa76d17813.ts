import {Location, LocationsAndGroups, LocationService} from './services/location.service';
import {Observable} from 'rxjs';
import {QueryService} from './services/query.service';
import {TypingSimulatorService} from './services/typing-simulator.service';
import {switchMap} from 'rxjs/operators';
import {TenantSettingsService} from './services/tenant-settings.service';

describe('Observables', () => {
  let locationService: LocationService;
  let queryService: QueryService;
  let tenantSettingsService: TenantSettingsService;
  let typingSimulatorService: TypingSimulatorService;
  const myNewLocation: Location = {id: 42, name: 'My Location'};
  const REPLACE_WITH_YOUR_RXJS_SOLUTION = {} as Observable<any>;

  beforeEach(() => {
    locationService = new LocationService();
    queryService = new QueryService();
    typingSimulatorService = new TypingSimulatorService();
    tenantSettingsService = new TenantSettingsService();

    jasmine.clock().install();
  });

  afterEach(() => {
    jasmine.clock().uninstall();
  });

  /**
   * Instructions:
   * We need the Locations returned from getLocations() to populate the LocationGroups returned from getLocationGroups()
   * Use:
   *  - locationService.getLocations()
   *  - locationService.getLocationGroups()
   */
  it('[Challenge 1]: Parallel Requests', () => {
    let result: LocationsAndGroups | undefined;

    REPLACE_WITH_YOUR_RXJS_SOLUTION
      .subscribe((value: LocationsAndGroups) => {
        result = value;
        // Now we could populate the locations inside of the locationsGroups with the instances
      });
    jasmine.clock().tick(100);

    expect(result?.locations).toHaveSize(3);
    expect(result?.groups).toHaveSize(2);
  });

  /**
   * Instructions:
   * When we change the current tenant we want to refetch the locations as we have another tenantId
   * This needs to happen continuously
   *
   * Use:
   *  tenantSettings.tenantSettingChanges() -> Triggers when the currently selected tenant changes (will trigger twice)
   *  locationService.getLocations()
   *
   */
  it('[Challenge 2]: Trigger re-fetch after TenantSettings changed', () => {
    let results: Location[][] = [];
    REPLACE_WITH_YOUR_RXJS_SOLUTION
      .subscribe((value: Location[]) => {
        results.push(value);
      });
    jasmine.clock().tick(100);
    expect(results).toHaveSize(1);

    // Trigger tenant change
    tenantSettingsService.changeTenant('my-new-tenant-id');
    jasmine.clock().tick(100);
    expect(results).toHaveSize(2);
  });

  /**
   * Instructions:
   * We want to update our Location and then reload the list of all locations in our location overview
   * Use:
   *  - locationService.updateLocation(myLocation)
   *  - locationService.getLocationGroups()
   * Bonus Points:
   *  - Use only one subscribe
   */
  it('[Challenge 3]: Observable with void return type', () => {
    let result: Location[] | undefined;

    REPLACE_WITH_YOUR_RXJS_SOLUTION
      .subscribe((value: Location[]) => {
        result = value;
      });
    jasmine.clock().tick(100);

    expect(result).toHaveSize(3);
    // @ts-ignore
    expect(result[4].name).toBe(myNewLocation.name);
  });

  /**
   * Instructions:
   * We want to search for autocomplete suggestions on the backend. To save backend resources we want to abort old requests
   * Use:
   *  simulator.onKeyUp$ -> For User inputs - no need to debounce
   *  queryService.lookupAutocompleteSuggestions(inputValue)
   *
   */
  it('[Challenge 4]: Aborting ongoing observables', () => {
    const simulator = typingSimulatorService.getTypingSimulator();

    let returnedResults: string[] | undefined = undefined;
    REPLACE_WITH_YOUR_RXJS_SOLUTION.subscribe(results => {
      returnedResults = results;
    });


    // H
    simulator.nextKeyStroke();
    expect(queryService.unsubscribes.length).toBe(0);

    // He
    simulator.nextKeyStroke();
    expect(queryService.unsubscribes.length).toBe(1);

    // Hel
    simulator.nextKeyStroke();
    expect(queryService.unsubscribes.length).toBe(2);

    // Hell
    simulator.nextKeyStroke();
    expect(queryService.unsubscribes.length).toBe(3);
    expect(returnedResults).toBeUndefined();

    // Allow the last input value "Hell" to be returning values by having the "debounceTime" to be passed
    simulator.waitForResults();

    expect(queryService.unsubscribes.length).toBe(4);
    expect(returnedResults as unknown as string[]).toHaveSize(3);
  });

});