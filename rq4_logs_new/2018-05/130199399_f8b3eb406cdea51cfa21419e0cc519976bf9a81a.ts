import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EditCodeModalComponent } from './edit-code-modal.component';

describe('EditCodeModalComponent', () => {
  let component: EditCodeModalComponent;
  let fixture: ComponentFixture<EditCodeModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EditCodeModalComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EditCodeModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});