import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ComponentRegisterComponent } from './component-register.component';

describe('ComponentRegisterComponent', () => {
  let component: ComponentRegisterComponent;
  let fixture: ComponentFixture<ComponentRegisterComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ComponentRegisterComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ComponentRegisterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});