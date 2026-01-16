import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HomeEditorComponent } from './home-editor.component';

describe('HomeEditorComponent', () => {
  let component: HomeEditorComponent;
  let fixture: ComponentFixture<HomeEditorComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HomeEditorComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HomeEditorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});