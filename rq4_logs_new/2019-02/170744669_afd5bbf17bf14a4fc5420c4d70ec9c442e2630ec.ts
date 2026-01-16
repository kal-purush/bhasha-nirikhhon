import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GroupActivitesComponent } from './group-activites.component';

describe('GroupActivitesComponent', () => {
  let component: GroupActivitesComponent;
  let fixture: ComponentFixture<GroupActivitesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GroupActivitesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GroupActivitesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});