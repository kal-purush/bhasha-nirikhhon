import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GetuigeComponent } from './getuige.component';

describe('GetuigeComponent', () => {
  let component: GetuigeComponent;
  let fixture: ComponentFixture<GetuigeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GetuigeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GetuigeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});