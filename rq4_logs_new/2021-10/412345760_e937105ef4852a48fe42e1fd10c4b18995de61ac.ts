import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DialogSearchUserComponent } from './dialog-search-user.component';

describe('DialogSearchUserComponent', () => {
  let component: DialogSearchUserComponent;
  let fixture: ComponentFixture<DialogSearchUserComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DialogSearchUserComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DialogSearchUserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});