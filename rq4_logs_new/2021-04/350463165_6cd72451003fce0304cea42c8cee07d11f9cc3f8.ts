import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HrFormComponent } from './hr-form.component';

describe('HrFormComponent', () => {
  let component: HrFormComponent;
  let fixture: ComponentFixture<HrFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HrFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HrFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});