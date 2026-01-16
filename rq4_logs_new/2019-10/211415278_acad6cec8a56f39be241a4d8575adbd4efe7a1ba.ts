import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { StrandedOkayComponent } from './stranded-okay.component';

describe('StrandedOkayComponent', () => {
  let component: StrandedOkayComponent;
  let fixture: ComponentFixture<StrandedOkayComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ StrandedOkayComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(StrandedOkayComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});