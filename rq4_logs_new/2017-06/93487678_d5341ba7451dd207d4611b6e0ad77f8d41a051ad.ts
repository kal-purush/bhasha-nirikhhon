import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PicArticleComponent } from './pic-article.component';

describe('PicArticleComponent', () => {
  let component: PicArticleComponent;
  let fixture: ComponentFixture<PicArticleComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PicArticleComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PicArticleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});