import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EditPrestationComponent } from './edit-prestation.component';

describe('EditPrestationComponent', () => {
  let component: EditPrestationComponent;
  let fixture: ComponentFixture<EditPrestationComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EditPrestationComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EditPrestationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});