import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FixedtopComponent } from './fixedtop.component';

describe('FixedtopComponent', () => {
  let component: FixedtopComponent;
  let fixture: ComponentFixture<FixedtopComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FixedtopComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FixedtopComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});