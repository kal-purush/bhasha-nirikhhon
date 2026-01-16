import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddingNewMovieComponent } from './adding-new-movie.component';

describe('AddingNewMovieComponent', () => {
  let component: AddingNewMovieComponent;
  let fixture: ComponentFixture<AddingNewMovieComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddingNewMovieComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddingNewMovieComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});