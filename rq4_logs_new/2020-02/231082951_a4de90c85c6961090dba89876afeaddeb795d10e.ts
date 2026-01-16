import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MaxRepComponent } from './max-rep.component';

describe('MaxRepComponent', () => {
  let component: MaxRepComponent;
  let fixture: ComponentFixture<MaxRepComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MaxRepComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MaxRepComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});