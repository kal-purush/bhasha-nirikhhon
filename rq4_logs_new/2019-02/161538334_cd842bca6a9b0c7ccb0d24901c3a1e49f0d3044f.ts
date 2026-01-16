import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ApiYoutubeVideoComponent } from './api-youtube-video.component';

describe('ApiYoutubeVideoComponent', () => {
  let component: ApiYoutubeVideoComponent;
  let fixture: ComponentFixture<ApiYoutubeVideoComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ApiYoutubeVideoComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ApiYoutubeVideoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});