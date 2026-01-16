import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ChooseAbilitiesDialogComponent } from './choose-abilities-dialog.component';

describe('ChooseAbilitiesDialogComponent', () => {
  let component: ChooseAbilitiesDialogComponent;
  let fixture: ComponentFixture<ChooseAbilitiesDialogComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ChooseAbilitiesDialogComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ChooseAbilitiesDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});