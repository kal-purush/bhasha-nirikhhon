import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProjectsPaneComponent } from './projects-pane.component';

describe('ProjectsPaneComponent', () => {
  let component: ProjectsPaneComponent;
  let fixture: ComponentFixture<ProjectsPaneComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ProjectsPaneComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProjectsPaneComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});