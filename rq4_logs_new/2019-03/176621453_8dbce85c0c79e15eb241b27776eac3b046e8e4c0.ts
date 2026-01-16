import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CapturePhonenumberComponent } from './capture-phonenumber.component';

describe('CapturePhonenumberComponent', () => {
  let component: CapturePhonenumberComponent;
  let fixture: ComponentFixture<CapturePhonenumberComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CapturePhonenumberComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CapturePhonenumberComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});