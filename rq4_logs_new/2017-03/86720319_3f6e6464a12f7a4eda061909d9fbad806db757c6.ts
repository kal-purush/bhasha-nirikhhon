import { Component, DebugElement, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DataService, ActionsService, StateManagerService } from '../../shared/services';
import { Job } from '../../shared/interfaces';
import { JobComponent } from './job.component';

@Component({
  selector: 'mock-parent',
  template: `
    <js-job [job]="job" (delete)="removeJob(1)"></js-job>
  `,
  providers: [
    DataService, ActionsService, StateManagerService
  ]
})
class MockParentComponent {
  public job: Job;

  constructor() {
    this.job =
      {
        "name": "test",
        "container_size": 1,
        "frequency": 1,
        "last_run": "Aug 12 2015",
        "next_due": "April 30th 2017",
        "command": "npm install"
      }
  }

  public removeJob(num: number) {

  }
}

describe('JobComponent', () => {
  let component: JobComponent;
  let fixture: ComponentFixture<MockParentComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [CommonModule],
      declarations: [ JobComponent, MockParentComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MockParentComponent);
    component = fixture.debugElement.children[0].componentInstance;
    fixture.detectChanges();

    spyOn(component.delete, 'emit');
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('Life Cycle Hooks', () => {
    it('should call / setup OnInit()', () => {
        expect(component.containerSizes).toEqual([
          {selected: false, name: 'UNLIMIT STACKS'},
          {selected: true, name: 'ONE STACK'},
          {selected: false, name: 'TWO STACKS'},
          {selected: false, name: 'THREE STACKS'}
        ]);

        expect(component.frequency).toEqual([
          {selected: false, name: 'DAILY'},
          {selected: true, name: 'HOURLY'},
          {selected: false, name: 'EVERY TEN MINUTES'}
        ]);
    });
  });

  describe('Custom Methods', () => {
    it('should update name - updateName()', () => {
      let event =  {
        target: {
          value : 'fake name'
        }
      };

      component.updateName(event);

      expect(component.job.name).toEqual('fake name');
    });

    it('should turn on edit mode - activateEditMode()', () => {
      component.activateEditMode();

      expect(component.isEditMode).toBe(true);
    });

    it('should remove a job - removeJob()', () => {
      component.removeJob();

      expect(component.delete.emit).toHaveBeenCalled();
    });
  });
});