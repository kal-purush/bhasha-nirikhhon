import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CrearReunionComponent } from './crear-reunion.component';

describe('CrearReunionComponent', () => {
  let component: CrearReunionComponent;
  let fixture: ComponentFixture<CrearReunionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CrearReunionComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CrearReunionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});