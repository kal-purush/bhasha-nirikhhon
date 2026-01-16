import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { XFloatingActionButtonComponent } from './x-floating-action-button.component';

describe('XFloatingActionButtonComponent', () => {
  let component: XFloatingActionButtonComponent;
  let fixture: ComponentFixture<XFloatingActionButtonComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ XFloatingActionButtonComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(XFloatingActionButtonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});