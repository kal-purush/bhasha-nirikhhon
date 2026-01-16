import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProjectSnippetComponent } from './project-snippet.component';

describe('ProjectSnippetComponent', () => {
  let component: ProjectSnippetComponent;
  let fixture: ComponentFixture<ProjectSnippetComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ProjectSnippetComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ProjectSnippetComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});