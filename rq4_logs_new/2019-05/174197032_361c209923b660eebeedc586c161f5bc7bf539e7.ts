import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChangeRentalServiceComponent } from './change-rental-service.component';

describe('ChangeRentalServiceComponent', () => {
  let component: ChangeRentalServiceComponent;
  let fixture: ComponentFixture<ChangeRentalServiceComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChangeRentalServiceComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChangeRentalServiceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});