import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BarcodeScannerDialogComponent } from './barcode-scanner-dialog.component';

describe('BarcodeScannerDialogComponent', () => {
  let component: BarcodeScannerDialogComponent;
  let fixture: ComponentFixture<BarcodeScannerDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BarcodeScannerDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BarcodeScannerDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});