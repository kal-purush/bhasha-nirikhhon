import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FunctionalityDetailComponent } from './functionality-detail.component';

describe('FunctionalityDetailComponent', () => {
  let component: FunctionalityDetailComponent;
  let fixture: ComponentFixture<FunctionalityDetailComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [FunctionalityDetailComponent]
    });
    fixture = TestBed.createComponent(FunctionalityDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});