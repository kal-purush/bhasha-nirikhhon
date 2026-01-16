import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TweetAloneComponent } from './tweet-alone.component';

describe('TweetAloneComponent', () => {
  let component: TweetAloneComponent;
  let fixture: ComponentFixture<TweetAloneComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TweetAloneComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TweetAloneComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});