import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ActionGridComponent } from './action-grid.component';

describe('ActionGridComponent', () => {
  let component: ActionGridComponent;
  let fixture: ComponentFixture<ActionGridComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ActionGridComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ActionGridComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});