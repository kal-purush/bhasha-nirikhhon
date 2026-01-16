import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AppleComponent } from './apple.component';

describe('AppleComponent', () => {
  let component: AppleComponent;
  let fixture: ComponentFixture<AppleComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AppleComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AppleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});