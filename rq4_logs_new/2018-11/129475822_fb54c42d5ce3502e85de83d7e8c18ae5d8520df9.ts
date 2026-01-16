import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddPlayerToNpcShiftComponent } from './add-player-to-npc-shift.component';

describe('AddPlayerToNpcShiftComponent', () => {
  let component: AddPlayerToNpcShiftComponent;
  let fixture: ComponentFixture<AddPlayerToNpcShiftComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddPlayerToNpcShiftComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddPlayerToNpcShiftComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});