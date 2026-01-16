import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ImagineFinalComponent } from './imagine-final.component';

describe('ImagineFinalComponent', () => {
  let component: ImagineFinalComponent;
  let fixture: ComponentFixture<ImagineFinalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ImagineFinalComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ImagineFinalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});