import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Sha512Component } from './sha512.component';

describe('Sha512Component', () => {
  let component: Sha512Component;
  let fixture: ComponentFixture<Sha512Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Sha512Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Sha512Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});