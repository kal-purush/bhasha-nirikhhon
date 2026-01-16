import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { XlayoutchangesComponent } from './xlayoutchanges.component';

describe('XlayoutchangesComponent', () => {
  let component: XlayoutchangesComponent;
  let fixture: ComponentFixture<XlayoutchangesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ XlayoutchangesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(XlayoutchangesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});