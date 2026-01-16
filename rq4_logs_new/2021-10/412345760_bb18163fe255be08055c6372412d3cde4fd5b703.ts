import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PerfilMainComponent } from './perfil-main.component';

describe('PerfilMainComponent', () => {
  let component: PerfilMainComponent;
  let fixture: ComponentFixture<PerfilMainComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ PerfilMainComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PerfilMainComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});