import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { HeroMissionComponent } from './hero-mission.component';

describe('HeroMissionComponent', () => {
  let component: HeroMissionComponent;
  let fixture: ComponentFixture<HeroMissionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HeroMissionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HeroMissionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});