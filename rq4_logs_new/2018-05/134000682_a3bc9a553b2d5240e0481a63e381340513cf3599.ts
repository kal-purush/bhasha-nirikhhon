import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NonProfitsComponent } from './non-profits.component';

describe('NonProfitsComponent', () => {
  let component: NonProfitsComponent;
  let fixture: ComponentFixture<NonProfitsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NonProfitsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NonProfitsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});