import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SomethingsomethingComponent } from './somethingsomething.component';

describe('SomethingsomethingComponent', () => {
  let component: SomethingsomethingComponent;
  let fixture: ComponentFixture<SomethingsomethingComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SomethingsomethingComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SomethingsomethingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});