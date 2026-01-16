import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ThematicDashboardComponent } from './thematic-dashboard.component';

describe('ThematicDashboardComponent', () => {
  let component: ThematicDashboardComponent;
  let fixture: ComponentFixture<ThematicDashboardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ThematicDashboardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ThematicDashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});