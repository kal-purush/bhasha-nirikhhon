import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ArticlFormComponent } from './articl-form.component';

describe('ArticlFormComponent', () => {
  let component: ArticlFormComponent;
  let fixture: ComponentFixture<ArticlFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ArticlFormComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ArticlFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});