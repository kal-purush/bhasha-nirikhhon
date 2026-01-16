import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NoteChooserComponent } from './note-chooser.component';

describe('NoteChooserComponent', () => {
  let component: NoteChooserComponent;
  let fixture: ComponentFixture<NoteChooserComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NoteChooserComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NoteChooserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});