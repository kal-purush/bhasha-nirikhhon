import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GixButtonTwolcornComponent } from './gix-button-twolcorn.component';

describe('GixButtonTwolcornComponent', () => {
  let component: GixButtonTwolcornComponent;
  let fixture: ComponentFixture<GixButtonTwolcornComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GixButtonTwolcornComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GixButtonTwolcornComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});