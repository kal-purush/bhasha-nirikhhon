import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RateuserComponent } from './rateuser.component';

describe('RateuserComponent', () => {
  let component: RateuserComponent;
  let fixture: ComponentFixture<RateuserComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RateuserComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RateuserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});