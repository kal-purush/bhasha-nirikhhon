import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ListGoalsComponent } from './list-goals.component';

describe('ListGoalsComponent', () => {
  let component: ListGoalsComponent;
  let fixture: ComponentFixture<ListGoalsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ListGoalsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ListGoalsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});