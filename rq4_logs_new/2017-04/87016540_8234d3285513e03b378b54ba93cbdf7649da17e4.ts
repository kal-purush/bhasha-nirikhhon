import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GitRepositoryCardComponent } from './git-repository-card.component';

describe('GitRepositoryCardComponent', () => {
  let component: GitRepositoryCardComponent;
  let fixture: ComponentFixture<GitRepositoryCardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GitRepositoryCardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GitRepositoryCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});