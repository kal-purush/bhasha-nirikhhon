import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EditDonateurComponent } from './edit-donateur.component';

describe('EditDonateurComponent', () => {
  let component: EditDonateurComponent;
  let fixture: ComponentFixture<EditDonateurComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EditDonateurComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EditDonateurComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});