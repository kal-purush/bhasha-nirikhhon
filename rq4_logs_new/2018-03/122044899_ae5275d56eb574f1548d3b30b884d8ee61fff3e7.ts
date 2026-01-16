import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddmatchPageComponent } from './addmatch-page.component';

describe('AddmatchPageComponent', () => {
  let component: AddmatchPageComponent;
  let fixture: ComponentFixture<AddmatchPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddmatchPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddmatchPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});