import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScrollButtonComponent } from './scroll-button.component';

describe('ScrollButtonComponent', () => {
  let component: ScrollButtonComponent;
  let fixture: ComponentFixture<ScrollButtonComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScrollButtonComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScrollButtonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});