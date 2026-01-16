import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChatLogComponent } from './chat-log.component';

describe('ChatLogComponent', () => {
  let component: ChatLogComponent;
  let fixture: ComponentFixture<ChatLogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChatLogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChatLogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});