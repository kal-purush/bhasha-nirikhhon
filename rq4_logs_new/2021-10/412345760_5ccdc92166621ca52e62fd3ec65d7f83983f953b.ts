import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DialogConfirmarRolComponent } from './dialog-confirmar-rol.component';

describe('DialogConfirmarRolComponent', () => {
  let component: DialogConfirmarRolComponent;
  let fixture: ComponentFixture<DialogConfirmarRolComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DialogConfirmarRolComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DialogConfirmarRolComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});