import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HomeChiefEditorComponent } from './home-chief-editor.component';

describe('HomeChiefEditorComponent', () => {
  let component: HomeChiefEditorComponent;
  let fixture: ComponentFixture<HomeChiefEditorComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HomeChiefEditorComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HomeChiefEditorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});