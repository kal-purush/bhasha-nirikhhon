import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CreditsOffersComponent } from './credits-offers.component';

describe('CreditsOffersComponent', () => {
  let component: CreditsOffersComponent;
  let fixture: ComponentFixture<CreditsOffersComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CreditsOffersComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CreditsOffersComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});