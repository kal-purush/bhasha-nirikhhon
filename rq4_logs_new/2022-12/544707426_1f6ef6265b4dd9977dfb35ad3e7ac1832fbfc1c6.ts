import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NghiPhepConfirmComponent } from './nghi-phep-confirm.component';

describe('NghiPhepConfirmComponent', () => {
  let component: NghiPhepConfirmComponent;
  let fixture: ComponentFixture<NghiPhepConfirmComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ NghiPhepConfirmComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NghiPhepConfirmComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});