import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BookingListModalComponent } from './booking-list-modal.component';

describe('BookingListModalComponent', () => {
  let component: BookingListModalComponent;
  let fixture: ComponentFixture<BookingListModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ BookingListModalComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BookingListModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});