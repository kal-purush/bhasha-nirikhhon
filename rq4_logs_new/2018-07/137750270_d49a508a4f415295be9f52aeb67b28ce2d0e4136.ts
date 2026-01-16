import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InputErrorWallabyComponent } from './input-error-wallaby.component';

describe('InputErrorWallabyComponent', () => {
  let component: InputErrorWallabyComponent;
  let fixture: ComponentFixture<InputErrorWallabyComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InputErrorWallabyComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InputErrorWallabyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});