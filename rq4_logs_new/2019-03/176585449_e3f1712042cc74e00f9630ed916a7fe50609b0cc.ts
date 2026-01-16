import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GixiconTableComponent } from './gixicon-table.component';

describe('GixiconTableComponent', () => {
  let component: GixiconTableComponent;
  let fixture: ComponentFixture<GixiconTableComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GixiconTableComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GixiconTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});