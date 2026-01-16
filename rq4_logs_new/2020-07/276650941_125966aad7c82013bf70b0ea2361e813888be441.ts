import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TxDialogComponent } from './tx-dialog.component';

describe('TxDialogComponent', () => {
  let component: TxDialogComponent;
  let fixture: ComponentFixture<TxDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TxDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TxDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});