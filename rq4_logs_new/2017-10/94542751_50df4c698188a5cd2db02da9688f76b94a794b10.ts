import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TradePortalComponent } from './trade-portal.component';

describe('TradePortalComponent', () => {
  let component: TradePortalComponent;
  let fixture: ComponentFixture<TradePortalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TradePortalComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TradePortalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});