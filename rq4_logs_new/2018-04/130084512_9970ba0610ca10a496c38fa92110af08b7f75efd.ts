import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NewsTitlesComponent } from './news-titles.component';

describe('NewsTitlesComponent', () => {
  let component: NewsTitlesComponent;
  let fixture: ComponentFixture<NewsTitlesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NewsTitlesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NewsTitlesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});