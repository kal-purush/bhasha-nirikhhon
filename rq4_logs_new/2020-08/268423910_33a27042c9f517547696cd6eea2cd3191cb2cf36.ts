import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ListTestRunsComponent } from './list-test-runs.component';

describe('ListTestRunsComponent', () => {
  let component: ListTestRunsComponent;
  let fixture: ComponentFixture<ListTestRunsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ListTestRunsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ListTestRunsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});