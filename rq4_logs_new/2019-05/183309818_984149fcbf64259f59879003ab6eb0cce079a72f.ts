import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AppClienteComponent } from './app-cliente.component';

describe('AppClienteComponent', () => {
  let component: AppClienteComponent;
  let fixture: ComponentFixture<AppClienteComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AppClienteComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AppClienteComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});