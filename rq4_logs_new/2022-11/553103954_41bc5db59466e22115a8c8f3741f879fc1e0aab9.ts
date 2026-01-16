import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DetalleTurnoComponent } from './detalle-turno.component';

describe('DetalleTurnoComponent', () => {
  let component: DetalleTurnoComponent;
  let fixture: ComponentFixture<DetalleTurnoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DetalleTurnoComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DetalleTurnoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});