import { ComponentFixture, TestBed } from '@angular/core/testing';

import { Account360Component } from './account360.component';

describe('Account360Component', () => {
  let component: Account360Component;
  let fixture: ComponentFixture<Account360Component>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ Account360Component ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(Account360Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});