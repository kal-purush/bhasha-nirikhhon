import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AutoExpressionComponent } from './auto-expression.component';

describe('AutoExpressionComponent', () => {
  let component: AutoExpressionComponent;
  let fixture: ComponentFixture<AutoExpressionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AutoExpressionComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AutoExpressionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});