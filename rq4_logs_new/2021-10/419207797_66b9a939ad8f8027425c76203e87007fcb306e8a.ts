import { ComponentFixture, TestBed } from '@angular/core/testing';

import { GetInvolvedFormComponent } from './get-involved-form.component';

describe('GetInvolvedFormComponent', () => {
  let component: GetInvolvedFormComponent;
  let fixture: ComponentFixture<GetInvolvedFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ GetInvolvedFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(GetInvolvedFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});