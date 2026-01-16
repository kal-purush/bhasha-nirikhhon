import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddStopFormComponent } from './add-stop-form.component';

describe('AddStopFormComponent', () => {
  let component: AddStopFormComponent;
  let fixture: ComponentFixture<AddStopFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddStopFormComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddStopFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});