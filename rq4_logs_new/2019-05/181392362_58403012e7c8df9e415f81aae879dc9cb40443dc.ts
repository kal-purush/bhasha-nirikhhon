import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { StandupDetailComponent } from './standup-detail.component';

describe('StandupDetailComponent', () => {
  let component: StandupDetailComponent;
  let fixture: ComponentFixture<StandupDetailComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ StandupDetailComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(StandupDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});