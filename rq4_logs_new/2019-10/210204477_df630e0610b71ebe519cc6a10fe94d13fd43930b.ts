import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { WheelOfSkillsComponent } from './wheel-of-skills.component';

describe('WheelOfSkillsComponent', () => {
  let component: WheelOfSkillsComponent;
  let fixture: ComponentFixture<WheelOfSkillsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ WheelOfSkillsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(WheelOfSkillsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});