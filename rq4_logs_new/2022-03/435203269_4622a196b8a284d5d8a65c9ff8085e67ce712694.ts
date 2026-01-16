import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ChangeCharacterImageDialogComponent } from './change-character-image-dialog.component';

describe('ChangeCharacterImageDialogComponent', () => {
  let component: ChangeCharacterImageDialogComponent;
  let fixture: ComponentFixture<ChangeCharacterImageDialogComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ChangeCharacterImageDialogComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ChangeCharacterImageDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});