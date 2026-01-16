import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ZoomHoverComponent } from './zoom-hover.component';

describe('ZoomHoverComponent', () => {
  let component: ZoomHoverComponent;
  let fixture: ComponentFixture<ZoomHoverComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ZoomHoverComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ZoomHoverComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});