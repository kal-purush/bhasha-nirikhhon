import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PenaltyChooserComponent } from './penalty-chooser.component';

describe('PenaltyChooserComponent', () => {
  let component: PenaltyChooserComponent;
  let fixture: ComponentFixture<PenaltyChooserComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PenaltyChooserComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PenaltyChooserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});