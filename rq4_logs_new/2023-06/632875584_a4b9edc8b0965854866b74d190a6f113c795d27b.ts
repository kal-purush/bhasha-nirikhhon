import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ChatSettingsBarComponent } from './chat-settings-bar.component';

describe('ChatSettingsBarComponent', () => {
  let component: ChatSettingsBarComponent;
  let fixture: ComponentFixture<ChatSettingsBarComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ChatSettingsBarComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ChatSettingsBarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});