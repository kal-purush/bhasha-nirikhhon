import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SearchbarVideoComponent } from './searchbar-video.component';

describe('SearchbarVideoComponent', () => {
  let component: SearchbarVideoComponent;
  let fixture: ComponentFixture<SearchbarVideoComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SearchbarVideoComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SearchbarVideoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});