import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BookSearchResultsComponent } from './book-search-results.component';

describe('BookSearchResultsComponent', () => {
  let component: BookSearchResultsComponent;
  let fixture: ComponentFixture<BookSearchResultsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BookSearchResultsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BookSearchResultsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});