import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddLaneComponent } from './add-lane.component';

describe('AddLaneComponent', () => {
  let component: AddLaneComponent;
  let fixture: ComponentFixture<AddLaneComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddLaneComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddLaneComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});