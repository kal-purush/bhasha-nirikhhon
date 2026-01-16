import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddCodeModalComponent } from './add-code-modal.component';

describe('AddCodeModalComponent', () => {
  let component: AddCodeModalComponent;
  let fixture: ComponentFixture<AddCodeModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddCodeModalComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddCodeModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});