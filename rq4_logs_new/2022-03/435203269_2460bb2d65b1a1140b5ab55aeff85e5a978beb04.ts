import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ChooseRitualsDialogComponent } from './choose-rituals-dialog.component';

describe('ChooseRitualsDialogComponent', () => {
  let component: ChooseRitualsDialogComponent;
  let fixture: ComponentFixture<ChooseRitualsDialogComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ChooseRitualsDialogComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ChooseRitualsDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});