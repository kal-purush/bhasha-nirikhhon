import {
  async,
  ComponentFixture,
  TestBed,
  inject} from '@angular/core/testing';
import { Observable } from 'rxjs';

import { JobDashboardComponent } from './job-dashboard.component';
import {
  DataService,
  StateManagerService,
  ActionsService } from '../shared/services';


describe('JobDashboardComponent', () => {
  let component: JobDashboardComponent;
  let fixture: ComponentFixture<JobDashboardComponent>;
  let dataService: DataService;
  let stateManager: StateManagerService;
  let fakeData = {
    "jobs" : [
      {
          "name": "test",
          "container_size": 1,
          "frequency": 1,
          "last_run": "",
          "next_due": "",
          "command": ""
      }
    ]
  };

  let spyGetJobsData = jasmine.createSpy('getJobsData');
  let spyGetModel = jasmine.createSpy('getModel');
  let spySetModel = jasmine.createSpy('setModel');

  class MockDataService {

    public getJobsData() {
      spyGetJobsData(fakeData);
      return Observable.of(fakeData);
    }
  }

  class MockStateManagerService {
    getModel(name) {
      spyGetModel(name);
      return Observable.of({});
    }

    setModel() {
      spySetModel();
    }
  }

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ JobDashboardComponent ],
      providers: [
        { provide: DataService, useClass: MockDataService },
        { provide: StateManagerService, useClass: MockStateManagerService },
        ActionsService
      ]
    })
    .compileComponents();
  });

  beforeEach(inject([DataService, StateManagerService],(_DataService_, _StateManagerService_) => {
    dataService = _DataService_;
    stateManager = _StateManagerService_;

    fixture = TestBed.createComponent(JobDashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

  }));

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('Life Cycle Hook Methods', () => {
    it('should setup model from OnInit() call', () => {
      expect(spyGetJobsData).toHaveBeenCalledWith(fakeData);
      expect(spyGetModel).toHaveBeenCalledWith('JobScheduler');
      expect(spySetModel).toHaveBeenCalled();
    });
  });

});