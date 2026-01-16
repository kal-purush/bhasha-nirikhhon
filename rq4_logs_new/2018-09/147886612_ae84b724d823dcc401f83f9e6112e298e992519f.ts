import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ComponentLoginComponent } from './component-login.component';

describe('ComponentLoginComponent', () => {
  let component: ComponentLoginComponent;
  let fixture: ComponentFixture<ComponentLoginComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ComponentLoginComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ComponentLoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});