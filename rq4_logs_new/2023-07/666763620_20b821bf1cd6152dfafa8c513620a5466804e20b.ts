import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TaskUpdateDialogComponent } from './task-update-dialog.component';

describe('TaskUpdateDialogComponent', () => {
  let component: TaskUpdateDialogComponent;
  let fixture: ComponentFixture<TaskUpdateDialogComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [TaskUpdateDialogComponent]
    });
    fixture = TestBed.createComponent(TaskUpdateDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});