import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ShaksCallToActionCompComponent } from './shaks-call-to-action-comp.component';

describe('ShaksCallToActionCompComponent', () => {
  let component: ShaksCallToActionCompComponent;
  let fixture: ComponentFixture<ShaksCallToActionCompComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ShaksCallToActionCompComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ShaksCallToActionCompComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});