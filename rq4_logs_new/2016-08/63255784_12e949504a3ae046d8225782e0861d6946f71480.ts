import {
  TestBed,
  inject
} from '@angular/core/testing';

import {
  SendMenuSelectionAction,
  MENU_SELECTION,
  TOKEN } from './send-menu-selection.action';
import {
  AppState,
  MenuSelection
} from '../model/app-state';
import { oneItem } from './../model/items.service.spec';
import { MockItemsService } from './../model/items.service.spec';
import { ItemsService } from './../model/items.service';
import { AppStateService } from '../model/app-state.service';
import { MenuSelectionSendAction } from './menu-selection-send.action';
import { ErrorBackendCallAction } from './error-backend-call.action';

describe('SendMenuSelection', () => {

  let mockService: MockItemsService;
  let action: SendMenuSelectionAction;
  let appStateService: AppStateService;

  let menuSelection = MenuSelection.createEmptyState();
  menuSelection.items = oneItem;
  menuSelection.contact = 'contact';
  menuSelection.countOfAtendies = '1';

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [  ],
      providers: [
        SendMenuSelectionAction,
        AppStateService,
        { provide: ItemsService, useClass: MockItemsService},
        { provide: TOKEN, useValue: 'toekn' },
        { provide: MENU_SELECTION, useValue: menuSelection}
      ]
    });
  });

  beforeEach(inject([ItemsService, SendMenuSelectionAction, AppStateService],
    (_mockService: ItemsService, _action: SendMenuSelectionAction, _appStateService: AppStateService ) => {
      mockService = <any> _mockService;
      action = _action;
      appStateService = _appStateService;
    }));

  it('should set the ui state to loading and forward to MenuSelectionSendAction', () => {

    spyOn(appStateService, 'dispatchAction');

    let initState = AppState.createEmptyState();

    let stateAfter = action.createNewState(initState);

    expect(stateAfter.uiState.isLoading).toBe(true, 'isLoading == true');
    expect(appStateService.dispatchAction).toHaveBeenCalledWith(MenuSelectionSendAction);

  });

  it('should set the uistate to loading and forward to ErrorBackendCallAction', () => {

    mockService.resultWithError = true;

    spyOn(appStateService, 'dispatchAction');

    let initState = AppState.createEmptyState();

    let stateAfter = action.createNewState(initState);

    expect(stateAfter.uiState.isLoading).toBe(true, 'isLoading == true');
    expect(appStateService.dispatchAction).toHaveBeenCalledWith(ErrorBackendCallAction);

  });

});