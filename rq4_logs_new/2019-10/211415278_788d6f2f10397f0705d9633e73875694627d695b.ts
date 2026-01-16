import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SafeMovingComponent } from './safe-moving.component';

describe('SafeMovingComponent', () => {
  let component: SafeMovingComponent;
  let fixture: ComponentFixture<SafeMovingComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SafeMovingComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SafeMovingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});