import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdminArticlesAddComponent } from './admin-articles-add.component';

describe('AdminArticlesAddComponent', () => {
  let component: AdminArticlesAddComponent;
  let fixture: ComponentFixture<AdminArticlesAddComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdminArticlesAddComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminArticlesAddComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});