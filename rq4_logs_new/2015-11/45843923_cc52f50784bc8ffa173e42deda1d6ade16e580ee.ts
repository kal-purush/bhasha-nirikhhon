import {
  describe,
  expect
} from 'angular2/testing';

import {VisibleComponent, VisibleEvent} from './visible-component';

describe('VisibleComponent', () => {

  let visibleComponent:VisibleComponent;

  beforeEach(() => {
    visibleComponent = new VisibleComponent();
  });

  it('should be fine', () => {
  	expect(visibleComponent).toBeDefined();
  });

  it ('should change internal state when show, hide and toggle methods called', () => {
    expect(visibleComponent.isShown).toBeFalsy();

    visibleComponent.show();
    expect(visibleComponent.isShown).toBeTruthy();
    
    visibleComponent.hide();
    expect(visibleComponent.isShown).toBeFalsy();
    
    visibleComponent.toggle();
    expect(visibleComponent.isShown).toBeTruthy();
  });
  
  it ('should change internal state when show method called', () => {
    visibleComponent.visibleEvent.toRx().subscribe((event:VisibleEvent) => {
      expect(event).toBe(VisibleEvent.SHOWN);
    });
    
    visibleComponent.show();
  });
  
  it ('should change internal state when hide method called', () => {
    visibleComponent.show();
    
    visibleComponent.visibleEvent.toRx().subscribe((event:VisibleEvent) => {
      expect(event).toBe(VisibleEvent.HIDDEN);
    });
    
    visibleComponent.hide();
  });
  
  it ('should change internal state when toggle method called', () => {
    visibleComponent.show();
    
    visibleComponent.visibleEvent.toRx().subscribe((event:VisibleEvent) => {
      expect(event).toBe(VisibleEvent.HIDDEN);
    });
    
    visibleComponent.toggle();
  });
});