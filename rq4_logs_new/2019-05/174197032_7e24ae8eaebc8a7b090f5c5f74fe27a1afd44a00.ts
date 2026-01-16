import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RateCarAndServiceComponent } from './rate-car-and-service.component';

describe('RateCarAndServiceComponent', () => {
  let component: RateCarAndServiceComponent;
  let fixture: ComponentFixture<RateCarAndServiceComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RateCarAndServiceComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RateCarAndServiceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});