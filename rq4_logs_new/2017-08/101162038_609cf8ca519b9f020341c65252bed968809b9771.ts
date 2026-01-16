import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ControlsidebarComponent } from './controlsidebar.component';

describe('ControlsidebarComponent', () => {
  let component: ControlsidebarComponent;
  let fixture: ComponentFixture<ControlsidebarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ControlsidebarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ControlsidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});