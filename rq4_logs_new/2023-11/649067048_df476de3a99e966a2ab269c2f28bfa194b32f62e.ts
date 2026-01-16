import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RegClientesComponent } from './reg-clientes.component';

describe('RegClientesComponent', () => {
  let component: RegClientesComponent;
  let fixture: ComponentFixture<RegClientesComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [RegClientesComponent]
    });
    fixture = TestBed.createComponent(RegClientesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});