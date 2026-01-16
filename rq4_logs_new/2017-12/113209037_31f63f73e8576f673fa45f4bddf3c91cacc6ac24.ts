import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EvalEventwiseRegisteredTeamsComponent } from './eval-eventwise-registered-teams.component';

describe('EvalEventwiseRegisteredTeamsComponent', () => {
  let component: EvalEventwiseRegisteredTeamsComponent;
  let fixture: ComponentFixture<EvalEventwiseRegisteredTeamsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EvalEventwiseRegisteredTeamsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EvalEventwiseRegisteredTeamsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});