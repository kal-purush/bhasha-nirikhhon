import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PositionSelectorComponent } from './position-selector.component';

describe('PositionSelectorComponent', () => {
  let component: PositionSelectorComponent;
  let fixture: ComponentFixture<PositionSelectorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PositionSelectorComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PositionSelectorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});