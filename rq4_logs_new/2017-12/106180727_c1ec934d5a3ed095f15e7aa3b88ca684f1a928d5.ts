import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MainpageViewComponent } from './mainpage-view.component';

describe('MainpageViewComponent', () => {
  let component: MainpageViewComponent;
  let fixture: ComponentFixture<MainpageViewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MainpageViewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MainpageViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});