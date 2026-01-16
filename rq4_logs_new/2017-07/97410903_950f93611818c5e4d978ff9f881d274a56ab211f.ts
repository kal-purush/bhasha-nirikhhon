import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RateseekerComponent } from './rateseeker.component';

describe('RateseekerComponent', () => {
  let component: RateseekerComponent;
  let fixture: ComponentFixture<RateseekerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RateseekerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RateseekerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});