import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MovingMotivationalComponent } from './moving-motivational.component';

describe('MovingMotivationalComponent', () => {
  let component: MovingMotivationalComponent;
  let fixture: ComponentFixture<MovingMotivationalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MovingMotivationalComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MovingMotivationalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});