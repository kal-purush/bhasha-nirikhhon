import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MovieCardContainerComponent } from './movie-card-container.component';

describe('MovieCardContainerComponent', () => {
  let component: MovieCardContainerComponent;
  let fixture: ComponentFixture<MovieCardContainerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MovieCardContainerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MovieCardContainerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});