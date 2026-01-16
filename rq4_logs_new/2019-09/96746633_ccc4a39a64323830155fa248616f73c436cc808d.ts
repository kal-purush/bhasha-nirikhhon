import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Film2019Component } from './film-2019.component';

describe('FilmsCategoriesComponent', () => {
  let component: Film2019Component;
  let fixture: ComponentFixture<Film2019Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [Film2019Component]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Film2019Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});