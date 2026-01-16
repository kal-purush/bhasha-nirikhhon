import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { StatusboardComponent } from './statusboard.component';

describe('StatusboardComponent', () => {
  let component: StatusboardComponent;
  let fixture: ComponentFixture<StatusboardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ StatusboardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(StatusboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});