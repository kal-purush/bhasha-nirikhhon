/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { LikeUnlikeComponent } from './like-unlike.component';

describe('LikeUnlikeComponent', () => {
  let component: LikeUnlikeComponent;
  let fixture: ComponentFixture<LikeUnlikeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LikeUnlikeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LikeUnlikeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});