import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TemplateDateTimePickerComponent } from './template-date-time-picker.component';

describe('TemplateDateTimePickerComponent', () => {
  let component: TemplateDateTimePickerComponent;
  let fixture: ComponentFixture<TemplateDateTimePickerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TemplateDateTimePickerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TemplateDateTimePickerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});