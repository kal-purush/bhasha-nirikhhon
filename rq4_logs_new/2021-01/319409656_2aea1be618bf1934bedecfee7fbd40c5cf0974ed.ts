import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BetaReaderBooksComponent } from './beta-reader-books.component';

describe('BetaReaderBooksComponent', () => {
  let component: BetaReaderBooksComponent;
  let fixture: ComponentFixture<BetaReaderBooksComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ BetaReaderBooksComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BetaReaderBooksComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});