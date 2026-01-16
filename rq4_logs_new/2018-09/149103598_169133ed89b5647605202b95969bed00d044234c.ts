import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DannytestComponent } from './dannytest.component';

describe('DannytestComponent', () => {
  let component: DannytestComponent;
  let fixture: ComponentFixture<DannytestComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DannytestComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DannytestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});