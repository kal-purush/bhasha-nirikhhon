import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ComponentEventPageComponent } from './component-event-page.component';

describe('ComponentEventPageComponent', () => {
  let component: ComponentEventPageComponent;
  let fixture: ComponentFixture<ComponentEventPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ComponentEventPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ComponentEventPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});