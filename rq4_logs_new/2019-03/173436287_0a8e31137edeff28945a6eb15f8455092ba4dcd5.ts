import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AssignSkusComponent } from './assign-skus.component';

describe('AssignSkusComponent', () => {
  let component: AssignSkusComponent;
  let fixture: ComponentFixture<AssignSkusComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AssignSkusComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AssignSkusComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});