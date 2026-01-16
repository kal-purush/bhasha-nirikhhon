import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReserveCarSpecialComponent } from './reserve-car-special.component';

describe('ReserveCarSpecialComponent', () => {
  let component: ReserveCarSpecialComponent;
  let fixture: ComponentFixture<ReserveCarSpecialComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReserveCarSpecialComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReserveCarSpecialComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});