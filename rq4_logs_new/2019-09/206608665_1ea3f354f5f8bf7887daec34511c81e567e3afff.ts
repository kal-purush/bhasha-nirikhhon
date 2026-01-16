import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EmulatorPageComponent } from './emulator-page.component';

describe('EmulatorPageComponent', () => {
  let component: EmulatorPageComponent;
  let fixture: ComponentFixture<EmulatorPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EmulatorPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EmulatorPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});