import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ArbolComponent } from './arbol-cuentas-contables.component';

describe('ArbolComponent', () => {
  let component: ArbolComponent;
  let fixture: ComponentFixture < ngx; -arbolComponent > ;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ArbolComponent ],
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ArbolComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});