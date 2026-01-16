import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GapRuleComponent } from './gap-rule.component';

describe('GapRuleComponent', () => {
  let component: GapRuleComponent;
  let fixture: ComponentFixture<GapRuleComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GapRuleComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GapRuleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});