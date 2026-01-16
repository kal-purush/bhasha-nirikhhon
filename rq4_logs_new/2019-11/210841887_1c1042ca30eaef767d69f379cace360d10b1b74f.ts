import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateVoDialogComponent } from './create-vo-dialog.component';

describe('CreateVoDialogComponent', () => {
  let component: CreateVoDialogComponent;
  let fixture: ComponentFixture<CreateVoDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CreateVoDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateVoDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});