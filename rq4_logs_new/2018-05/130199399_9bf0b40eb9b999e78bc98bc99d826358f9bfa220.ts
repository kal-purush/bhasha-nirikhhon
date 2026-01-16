import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AangifteComponent } from './aangifte.component';

describe('AangifteComponent', () => {
  let component: AangifteComponent;
  let fixture: ComponentFixture<AangifteComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AangifteComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AangifteComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});