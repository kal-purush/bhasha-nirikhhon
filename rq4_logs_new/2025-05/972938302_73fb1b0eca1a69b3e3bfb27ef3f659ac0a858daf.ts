import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EfficiencyGraphComponent } from './efficiency-graph.component';

describe('EfficiencyGraphComponent', () => {
  let component: EfficiencyGraphComponent;
  let fixture: ComponentFixture<EfficiencyGraphComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [EfficiencyGraphComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(EfficiencyGraphComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});