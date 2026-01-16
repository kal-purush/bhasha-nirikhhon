import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ArticleViewListComponent } from './article-view-list.component';

describe('ArticleViewListComponent', () => {
  let component: ArticleViewListComponent;
  let fixture: ComponentFixture<ArticleViewListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ArticleViewListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ArticleViewListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});