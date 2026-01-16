import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InquoteComponent } from './inquote.component';

describe('InquoteComponent', () => {
  let component: InquoteComponent;
  let fixture: ComponentFixture<InquoteComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InquoteComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InquoteComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});