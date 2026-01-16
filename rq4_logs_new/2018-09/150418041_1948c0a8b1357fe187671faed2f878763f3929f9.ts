import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CountdownEntityComponent } from './countdown-entity.component';

describe('CountdownEntityComponent', () => {
  let component: CountdownEntityComponent;
  let fixture: ComponentFixture<CountdownEntityComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CountdownEntityComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CountdownEntityComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});