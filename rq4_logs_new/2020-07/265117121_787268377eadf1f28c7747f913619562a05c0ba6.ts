import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ArticlesListContainerComponent } from './articles-list-container.component';

describe('ArticlesListContainerComponent', () => {
  let component: ArticlesListContainerComponent;
  let fixture: ComponentFixture<ArticlesListContainerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ArticlesListContainerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ArticlesListContainerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});