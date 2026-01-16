import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ConsEncargoComponent } from './cons-encargo.component';

describe('ConsEncargoComponent', () => {
  let component: ConsEncargoComponent;
  let fixture: ComponentFixture<ConsEncargoComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ConsEncargoComponent]
    });
    fixture = TestBed.createComponent(ConsEncargoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});