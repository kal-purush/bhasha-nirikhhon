import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ApproveRejectButtonComponent } from './approve-reject-button.component';

describe('ApproveRejectButtonComponent', () => {
  let component: ApproveRejectButtonComponent;
  let fixture: ComponentFixture<ApproveRejectButtonComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ApproveRejectButtonComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ApproveRejectButtonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});