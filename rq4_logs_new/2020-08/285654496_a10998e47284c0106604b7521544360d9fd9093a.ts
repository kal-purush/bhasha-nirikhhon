import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CountrySwitcherComponent } from './country-switcher.component';

describe('CountrySwitcherComponent', () => {
  let component: CountrySwitcherComponent;
  let fixture: ComponentFixture<CountrySwitcherComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [CountrySwitcherComponent],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CountrySwitcherComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});