import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TestDiceComponent } from './test-dice.component';

describe('TestDiceComponent', () => {
  let component: TestDiceComponent;
  let fixture: ComponentFixture<TestDiceComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TestDiceComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TestDiceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});