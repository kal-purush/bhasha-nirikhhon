import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { IssueDataTableComponent } from './issue-data-table.component';

describe('IssueDataTableComponent', () => {
  let component: IssueDataTableComponent;
  let fixture: ComponentFixture<IssueDataTableComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ IssueDataTableComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(IssueDataTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});