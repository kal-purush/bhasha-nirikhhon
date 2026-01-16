import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PpBooksComponent } from './pp-books.component';

describe('PpBooksComponent', () => {
  let component: PpBooksComponent;
  let fixture: ComponentFixture<PpBooksComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PpBooksComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PpBooksComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});