import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdViewComponent } from './ad-view.component';

describe('AdViewComponent', () => {
  let component: AdViewComponent;
  let fixture: ComponentFixture<AdViewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdViewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});