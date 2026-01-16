import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProjectTipsComponent } from './project-tips.component';

describe('ProjectTipsComponent', () => {
  let component: ProjectTipsComponent;
  let fixture: ComponentFixture<ProjectTipsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ProjectTipsComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ProjectTipsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});