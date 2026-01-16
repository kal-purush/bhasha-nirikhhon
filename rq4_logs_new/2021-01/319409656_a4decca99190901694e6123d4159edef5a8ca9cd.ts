import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RefuseReasonComponent } from './refuse-reason.component';

describe('RefuseReasonComponent', () => {
  let component: RefuseReasonComponent;
  let fixture: ComponentFixture<RefuseReasonComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ RefuseReasonComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RefuseReasonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});