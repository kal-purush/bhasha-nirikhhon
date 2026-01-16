import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateRitualDialogComponent } from './create-ritual-dialog.component';

describe('CreateRitualDialogComponent', () => {
  let component: CreateRitualDialogComponent;
  let fixture: ComponentFixture<CreateRitualDialogComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CreateRitualDialogComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateRitualDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});