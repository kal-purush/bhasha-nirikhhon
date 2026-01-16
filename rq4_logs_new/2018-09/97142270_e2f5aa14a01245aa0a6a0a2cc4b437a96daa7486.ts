import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChildWindowComponent } from './child-window.component';

describe('ChildWindowComponent', () => {
  let component: ChildWindowComponent;
  let fixture: ComponentFixture<ChildWindowComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChildWindowComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChildWindowComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});