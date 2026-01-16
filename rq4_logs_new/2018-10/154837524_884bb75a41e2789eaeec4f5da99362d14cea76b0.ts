import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InquiryChooserComponent } from './inquiry-chooser.component';

describe('InquiryChooserComponent', () => {
  let component: InquiryChooserComponent;
  let fixture: ComponentFixture<InquiryChooserComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InquiryChooserComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InquiryChooserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});