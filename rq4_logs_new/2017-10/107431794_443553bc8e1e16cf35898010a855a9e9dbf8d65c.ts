import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DefensePlayerComponent } from './defense-player.component';

describe('DefensePlayerComponent', () => {
  let component: DefensePlayerComponent;
  let fixture: ComponentFixture<DefensePlayerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DefensePlayerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DefensePlayerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});