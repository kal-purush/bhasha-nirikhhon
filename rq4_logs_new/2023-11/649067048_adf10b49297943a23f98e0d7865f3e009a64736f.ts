import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ConsultarNcComponent } from './consultar-nc.component';

describe('ConsultarNcComponent', () => {
  let component: ConsultarNcComponent;
  let fixture: ComponentFixture<ConsultarNcComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ConsultarNcComponent]
    });
    fixture = TestBed.createComponent(ConsultarNcComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});