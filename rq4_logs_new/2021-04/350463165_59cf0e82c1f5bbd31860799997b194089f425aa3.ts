import { ComponentFixture, TestBed } from '@angular/core/testing';

import { IntrviewerAddPageComponent } from './intrviewer-add-page.component';

describe('IntrviewerAddPageComponent', () => {
  let component: IntrviewerAddPageComponent;
  let fixture: ComponentFixture<IntrviewerAddPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ IntrviewerAddPageComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IntrviewerAddPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});