import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { QAComponent } from './q-a.component';

describe('QAComponent', () => {
  let component: QAComponent;
  let fixture: ComponentFixture<QAComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ QAComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(QAComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});