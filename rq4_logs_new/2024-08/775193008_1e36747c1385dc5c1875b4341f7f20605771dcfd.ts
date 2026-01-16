import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ListVisualizationComponent } from './list-visualization.component';

describe('ListVisualizationComponent', () => {
  let component: ListVisualizationComponent;
  let fixture: ComponentFixture<ListVisualizationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ListVisualizationComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(ListVisualizationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});