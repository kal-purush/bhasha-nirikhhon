import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RegNeComponent } from './reg-ne.component';

describe('RegNeComponent', () => {
  let component: RegNeComponent;
  let fixture: ComponentFixture<RegNeComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [RegNeComponent]
    });
    fixture = TestBed.createComponent(RegNeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});