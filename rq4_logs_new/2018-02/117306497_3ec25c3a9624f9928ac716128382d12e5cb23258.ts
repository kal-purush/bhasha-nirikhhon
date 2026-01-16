import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ComposeMessageDialogComponent } from './compose-message-dialog.component';

describe('ComposeMessageDialogComponent', () => {
  let component: ComposeMessageDialogComponent;
  let fixture: ComponentFixture<ComposeMessageDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ComposeMessageDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ComposeMessageDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});