import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { WorkflowManagerComponent } from './workflow-manager.component';

describe('WorkflowManagerComponent', () => {
  let component: WorkflowManagerComponent;
  let fixture: ComponentFixture<WorkflowManagerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ WorkflowManagerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(WorkflowManagerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});