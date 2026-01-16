import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ContactNeedHelpComponent } from './contact-need-help.component';

describe('ContactNeedHelpComponent', () => {
  let component: ContactNeedHelpComponent;
  let fixture: ComponentFixture<ContactNeedHelpComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ContactNeedHelpComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ContactNeedHelpComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});