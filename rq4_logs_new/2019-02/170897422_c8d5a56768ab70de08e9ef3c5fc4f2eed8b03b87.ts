import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { WindowPaneComponent } from './window-pane.component';

describe('WindowPaneComponent', () => {
  let component: WindowPaneComponent;
  let fixture: ComponentFixture<WindowPaneComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ WindowPaneComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(WindowPaneComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});