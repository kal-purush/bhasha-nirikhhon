import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ComponentEventComponent } from './component-event.component';

describe('ComponentEventComponent', () => {
  let component: ComponentEventComponent;
  let fixture: ComponentFixture<ComponentEventComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ComponentEventComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ComponentEventComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});