import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReusableDishwareComponent } from './reusable-dishware.component';

describe('ReusableDishwareComponent', () => {
  let component: ReusableDishwareComponent;
  let fixture: ComponentFixture<ReusableDishwareComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReusableDishwareComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReusableDishwareComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});