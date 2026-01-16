import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FunctionalityUpdateDialogComponent } from './functionality-update-dialog.component';

describe('FunctionalityUpdateDialogComponent', () => {
  let component: FunctionalityUpdateDialogComponent;
  let fixture: ComponentFixture<FunctionalityUpdateDialogComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [FunctionalityUpdateDialogComponent]
    });
    fixture = TestBed.createComponent(FunctionalityUpdateDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});