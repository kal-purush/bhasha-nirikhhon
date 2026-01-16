import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DialogCommonComponent } from './dialog-common.component';

describe('DialogCommonComponent', () => {
  let component: DialogCommonComponent;
  let fixture: ComponentFixture<DialogCommonComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DialogCommonComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DialogCommonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});