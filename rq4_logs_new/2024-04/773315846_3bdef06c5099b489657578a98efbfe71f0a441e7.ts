import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PredmetAlgo1Component } from './predmet-algo1.component';

describe('PredmetAlgo1Component', () => {
  let component: PredmetAlgo1Component;
  let fixture: ComponentFixture<PredmetAlgo1Component>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PredmetAlgo1Component]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(PredmetAlgo1Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});