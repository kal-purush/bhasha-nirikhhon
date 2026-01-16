import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { QuestionEditorSelectorComponent } from './question-editor-selector.component';

describe('QuestionEditorSelectorComponent', () => {
  let component: QuestionEditorSelectorComponent;
  let fixture: ComponentFixture<QuestionEditorSelectorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ QuestionEditorSelectorComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(QuestionEditorSelectorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});