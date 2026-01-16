import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReusableDishwareQuestionnaireComponent } from './reusable-dishware-questionnaire.component';

describe('ReusableDishwareQuestionnaireComponent', () => {
  let component: ReusableDishwareQuestionnaireComponent;
  let fixture: ComponentFixture<ReusableDishwareQuestionnaireComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReusableDishwareQuestionnaireComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReusableDishwareQuestionnaireComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});