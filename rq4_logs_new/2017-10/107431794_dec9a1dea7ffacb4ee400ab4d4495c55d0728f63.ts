import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DiscardConfirmationComponent } from './discard-confirmation.component';

describe('DiscardConfirmationComponent', () => {
  let component: DiscardConfirmationComponent;
  let fixture: ComponentFixture<DiscardConfirmationComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DiscardConfirmationComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DiscardConfirmationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});