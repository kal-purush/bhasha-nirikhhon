import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NegotiumTaskDetailsComponent } from './negotium-task-details.component';

describe('NegotiumTaskDetailsComponent', () => {
  let component: NegotiumTaskDetailsComponent;
  let fixture: ComponentFixture<NegotiumTaskDetailsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NegotiumTaskDetailsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NegotiumTaskDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});