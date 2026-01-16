import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ControlledInputComponent } from './controlled-input.component';

describe('ControlledInputComponent', () => {
  let component: ControlledInputComponent;
  let fixture: ComponentFixture<ControlledInputComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ControlledInputComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ControlledInputComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});