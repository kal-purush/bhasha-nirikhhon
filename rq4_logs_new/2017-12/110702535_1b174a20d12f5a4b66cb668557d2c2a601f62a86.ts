import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LibraryResultsComponent } from './library-results.component';

describe('LibraryResultsComponent', () => {
  let component: LibraryResultsComponent;
  let fixture: ComponentFixture<LibraryResultsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LibraryResultsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LibraryResultsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});