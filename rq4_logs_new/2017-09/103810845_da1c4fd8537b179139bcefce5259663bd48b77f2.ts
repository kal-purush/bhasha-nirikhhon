import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SalesOrderNewComponent } from './sales-order-new.component';

describe('SalesOrderNewComponent', () => {
  let component: SalesOrderNewComponent;
  let fixture: ComponentFixture<SalesOrderNewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SalesOrderNewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SalesOrderNewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});