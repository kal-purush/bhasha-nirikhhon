import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ApiYoutubeComponent } from './api-youtube.component';

describe('ApiYoutubeComponent', () => {
  let component: ApiYoutubeComponent;
  let fixture: ComponentFixture<ApiYoutubeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ApiYoutubeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ApiYoutubeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});