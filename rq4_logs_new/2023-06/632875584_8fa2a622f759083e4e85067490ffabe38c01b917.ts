import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PostToolbarComponent } from './post-toolbar.component';

describe('PostToolbarComponent', () => {
  let component: PostToolbarComponent;
  let fixture: ComponentFixture<PostToolbarComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ PostToolbarComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(PostToolbarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});