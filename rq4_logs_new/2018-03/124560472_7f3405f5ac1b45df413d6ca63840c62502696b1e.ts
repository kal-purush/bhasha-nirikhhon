import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SkillsSvgComponent } from './skills-svg.component';

describe('SkillsSvgComponent', () => {
  let component: SkillsSvgComponent;
  let fixture: ComponentFixture<SkillsSvgComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SkillsSvgComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SkillsSvgComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});