import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InputAvalibilityComponent } from './input-avalibility.component';

describe('InputAvalibilityComponent', () => {
  let component: InputAvalibilityComponent;
  let fixture: ComponentFixture<InputAvalibilityComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InputAvalibilityComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InputAvalibilityComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});