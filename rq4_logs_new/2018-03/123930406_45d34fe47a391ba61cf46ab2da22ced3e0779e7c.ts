import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BaseUserDialogComponent } from './base-user-dialog.component';

describe('BaseUserDialogComponent', () => {
  let component: BaseUserDialogComponent;
  let fixture: ComponentFixture<BaseUserDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BaseUserDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BaseUserDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});