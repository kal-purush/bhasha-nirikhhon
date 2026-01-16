import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AssotiativeValuesTableComponent } from './assotiative-values-table.component';

describe('AssotiativeValuesTableComponent', () => {
  let component: AssotiativeValuesTableComponent;
  let fixture: ComponentFixture<AssotiativeValuesTableComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AssotiativeValuesTableComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AssotiativeValuesTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});