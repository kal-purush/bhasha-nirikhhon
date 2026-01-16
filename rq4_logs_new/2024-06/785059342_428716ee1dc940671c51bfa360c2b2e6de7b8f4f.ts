import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AlterTripComponent } from './alter-trip.component';

describe('AlterTripComponent', () => {
  let component: AlterTripComponent;
  let fixture: ComponentFixture<AlterTripComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [AlterTripComponent],
    }).compileComponents();

    fixture = TestBed.createComponent(AlterTripComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});