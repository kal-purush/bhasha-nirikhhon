import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CrearNcComponent } from './crear-nc.component';

describe('CrearNcComponent', () => {
  let component: CrearNcComponent;
  let fixture: ComponentFixture<CrearNcComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [CrearNcComponent]
    });
    fixture = TestBed.createComponent(CrearNcComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});