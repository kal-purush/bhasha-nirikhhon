import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LineProgressComponent } from './line-progress.component';

describe('LineProgressComponent', () => {
  let component: LineProgressComponent;
  let fixture: ComponentFixture<LineProgressComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LineProgressComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LineProgressComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});