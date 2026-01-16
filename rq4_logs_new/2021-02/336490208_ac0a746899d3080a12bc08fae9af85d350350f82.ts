import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AppliedbyComponent } from './appliedby.component';

describe('AppliedbyComponent', () => {
  let component: AppliedbyComponent;
  let fixture: ComponentFixture<AppliedbyComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AppliedbyComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AppliedbyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});