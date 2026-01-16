import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TrailsOfUserComponent } from './trails-of-user.component';

describe('TrailsOfUserComponent', () => {
  let component: TrailsOfUserComponent;
  let fixture: ComponentFixture<TrailsOfUserComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TrailsOfUserComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TrailsOfUserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});