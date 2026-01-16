import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NewBookSearcherComponent } from './new-book-searcher.component';

describe('NewBookSearcherComponent', () => {
  let component: NewBookSearcherComponent;
  let fixture: ComponentFixture<NewBookSearcherComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NewBookSearcherComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NewBookSearcherComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});