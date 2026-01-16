import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BillLineListComponent } from './bill-line-list.component';

describe('BillLineListComponent', () => {
  let component: BillLineListComponent;
  let fixture: ComponentFixture<BillLineListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BillLineListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BillLineListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});