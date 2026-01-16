import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SkillsPaneComponent } from './skills-pane.component';

describe('SkillsPaneComponent', () => {
  let component: SkillsPaneComponent;
  let fixture: ComponentFixture<SkillsPaneComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SkillsPaneComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SkillsPaneComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});