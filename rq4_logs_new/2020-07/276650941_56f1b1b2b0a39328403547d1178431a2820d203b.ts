import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScanDialogComponent } from './scan-dialog.component';

describe('ScanDialogComponent', () => {
  let component: ScanDialogComponent;
  let fixture: ComponentFixture<ScanDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScanDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScanDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});