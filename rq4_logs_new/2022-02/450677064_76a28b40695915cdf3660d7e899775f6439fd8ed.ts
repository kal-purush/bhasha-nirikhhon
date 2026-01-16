import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PreconisationGlobalFormComponent } from './preconisation-global-form.component';

describe('PreconisationGlobalFormComponent', () => {
  let component: PreconisationGlobalFormComponent;
  let fixture: ComponentFixture<PreconisationGlobalFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ PreconisationGlobalFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PreconisationGlobalFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});