import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { VisualizarMateriaisComponent } from './visualizar-materiais.component';

describe('VisualizarMateriaisComponent', () => {
  let component: VisualizarMateriaisComponent;
  let fixture: ComponentFixture<VisualizarMateriaisComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ VisualizarMateriaisComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VisualizarMateriaisComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});