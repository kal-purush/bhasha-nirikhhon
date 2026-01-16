import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CustomFormTagComponent } from './custom-form-tag.component';

describe('CustomFormTagComponent', () => {
  let component: CustomFormTagComponent;
  let fixture: ComponentFixture<CustomFormTagComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CustomFormTagComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CustomFormTagComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});