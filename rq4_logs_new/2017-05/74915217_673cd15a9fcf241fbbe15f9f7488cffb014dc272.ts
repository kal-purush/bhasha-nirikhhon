import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgProgressbarComponent } from './ng-progressbar.component';

describe('NgProgressbarComponent', () => {
  let component: NgProgressbarComponent;
  let fixture: ComponentFixture<NgProgressbarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgProgressbarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgProgressbarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});