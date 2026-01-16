import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { YoutubeplayerComponent } from './youtubeplayer.component';

describe('YoutubeplayerComponent', () => {
  let component: YoutubeplayerComponent;
  let fixture: ComponentFixture<YoutubeplayerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ YoutubeplayerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(YoutubeplayerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});