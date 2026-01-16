import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChangePhotoPage } from './change-photo.page';

describe('ChangePhotoPage', () => {
  let component: ChangePhotoPage;
  let fixture: ComponentFixture<ChangePhotoPage>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChangePhotoPage ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChangePhotoPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});