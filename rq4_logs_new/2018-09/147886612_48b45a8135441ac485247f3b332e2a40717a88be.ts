import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ComponentFeedComponent } from './component-feed.component';

describe('ComponentFeedComponent', () => {
  let component: ComponentFeedComponent;
  let fixture: ComponentFixture<ComponentFeedComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ComponentFeedComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ComponentFeedComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});