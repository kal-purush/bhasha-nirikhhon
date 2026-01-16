import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ThreemodelComponent } from './threemodel.component';

describe('ThreemodelComponent', () => {
  let component: ThreemodelComponent;
  let fixture: ComponentFixture<ThreemodelComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ThreemodelComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ThreemodelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});