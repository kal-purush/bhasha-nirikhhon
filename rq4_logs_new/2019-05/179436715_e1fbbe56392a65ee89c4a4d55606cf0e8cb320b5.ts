import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BlogTagComponent } from './blog-tag.component';

describe('BlogTagComponent', () => {
  let component: BlogTagComponent;
  let fixture: ComponentFixture<BlogTagComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BlogTagComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BlogTagComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});