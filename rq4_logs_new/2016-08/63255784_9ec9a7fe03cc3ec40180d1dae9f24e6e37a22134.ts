import {
  inject,
  TestBed
} from '@angular/core/testing';

import { AppStateService, IAppAction } from './app-state.service';
import { CourseItem } from './course-item';
import { MenuSelection } from './menuSelection';

let oneItem = new CourseItem('theTitle', 'expl.');

class TestAction implements IAppAction {

  public createNewState(oldState) {

    let newState: MenuSelection = {
      items: [...oldState.items, oneItem],
      contact: oldState.contact,
      countOfAtendies: oldState.countOfAttendies
    };

    return newState;
  }
}

describe('AppStateService', () => {

  let appStateService: AppStateService;

  beforeEach( () => {
    TestBed.configureTestingModule({
      providers: [AppStateService]
    });
  });

  beforeEach(inject([AppStateService], (_appStateService: AppStateService) => {
    appStateService = _appStateService;
  }));


  it('should have an initial empty array as state', ( done ) => {
    appStateService.getAppState().subscribe( (appState) => {

      expect(appState.items).toEqual([]);
      done();

    });

  });

  it('should add items that are loaded from the backend', ( done ) => {

    appStateService.dispatchAction(new TestAction());

    const appState = appStateService.getLastAppState();

    expect(appState.items).toEqual([oneItem]);
    done();
  });

});
