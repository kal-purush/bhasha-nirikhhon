import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TrailsByIdComponent } from './trails-by-id.component';

describe('TrailsByIdComponent', () => {
  let component: TrailsByIdComponent;
  let fixture: ComponentFixture<TrailsByIdComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TrailsByIdComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TrailsByIdComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});