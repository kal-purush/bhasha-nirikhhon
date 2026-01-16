import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RemoveVoDialogComponent } from './remove-vo-dialog.component';

describe('RemoveVoDialogComponent', () => {
  let component: RemoveVoDialogComponent;
  let fixture: ComponentFixture<RemoveVoDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RemoveVoDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RemoveVoDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});