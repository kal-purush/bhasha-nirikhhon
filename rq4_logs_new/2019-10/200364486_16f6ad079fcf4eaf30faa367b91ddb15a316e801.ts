import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CalculateexpenseComponent } from './calculateexpense.component';

describe('CalculateexpenseComponent', () => {
  let component: CalculateexpenseComponent;
  let fixture: ComponentFixture<CalculateexpenseComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CalculateexpenseComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CalculateexpenseComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});