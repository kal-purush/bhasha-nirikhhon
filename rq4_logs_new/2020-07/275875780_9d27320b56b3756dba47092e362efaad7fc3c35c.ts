import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OtherprofileUserComponent } from './otherprofile-user.component';

describe('OtherprofileUserComponent', () => {
  let component: OtherprofileUserComponent;
  let fixture: ComponentFixture<OtherprofileUserComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OtherprofileUserComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OtherprofileUserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});