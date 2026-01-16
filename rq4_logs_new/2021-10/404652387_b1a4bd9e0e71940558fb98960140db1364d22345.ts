import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SuggestionChatsComponent } from './suggestion-chats.component';

describe('SuggestionChatsComponent', () => {
  let component: SuggestionChatsComponent;
  let fixture: ComponentFixture<SuggestionChatsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SuggestionChatsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SuggestionChatsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});