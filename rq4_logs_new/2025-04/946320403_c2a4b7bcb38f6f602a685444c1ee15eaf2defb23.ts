import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ButtonChatComponent } from './button-chat.component';

describe('ButtonChatComponent', () => {
  let component: ButtonChatComponent;
  let fixture: ComponentFixture<ButtonChatComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ButtonChatComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ButtonChatComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});