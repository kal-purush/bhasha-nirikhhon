import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TopMessageComponent } from './top-message.component';

describe('TopMessageComponent', () => {
  let component: TopMessageComponent;
  let fixture: ComponentFixture<TopMessageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TopMessageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TopMessageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});