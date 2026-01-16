import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ChatSelectComponent } from './chat-select.component';

describe('ChatSelectComponent', () => {
  let component: ChatSelectComponent;
  let fixture: ComponentFixture<ChatSelectComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ChatSelectComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ChatSelectComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});