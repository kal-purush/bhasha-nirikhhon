import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CrearEmComponent } from './crear-em.component';

describe('CrearEmComponent', () => {
  let component: CrearEmComponent;
  let fixture: ComponentFixture<CrearEmComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [CrearEmComponent]
    });
    fixture = TestBed.createComponent(CrearEmComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});