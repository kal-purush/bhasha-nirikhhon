import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { StatCardGridListComponent } from './stat-card-grid-list.component';

describe('StatCardGridListComponent', () => {
  let component: StatCardGridListComponent;
  let fixture: ComponentFixture<StatCardGridListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ StatCardGridListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(StatCardGridListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});