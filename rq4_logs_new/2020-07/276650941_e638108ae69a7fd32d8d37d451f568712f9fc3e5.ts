import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReceiveDialogComponent } from './receive-dialog.component';

describe('ReceiveDialogComponent', () => {
  let component: ReceiveDialogComponent;
  let fixture: ComponentFixture<ReceiveDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReceiveDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReceiveDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});