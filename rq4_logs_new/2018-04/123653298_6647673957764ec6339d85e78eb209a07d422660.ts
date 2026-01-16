import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AnalysisAddComponent } from './analysis-add.component';

describe('AnalysisAddComponent', () => {
  let component: AnalysisAddComponent;
  let fixture: ComponentFixture<AnalysisAddComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AnalysisAddComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AnalysisAddComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});