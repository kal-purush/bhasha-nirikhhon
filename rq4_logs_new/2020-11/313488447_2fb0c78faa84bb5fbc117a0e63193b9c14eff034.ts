import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BodyDonationComponent } from './body-donation.component';

describe('BodyDonationComponent', () => {
  let component: BodyDonationComponent;
  let fixture: ComponentFixture<BodyDonationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ BodyDonationComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BodyDonationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});