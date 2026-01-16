import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateConversationComponent } from './create-conversation.component';

describe('CreateConversationComponent', () => {
  let component: CreateConversationComponent;
  let fixture: ComponentFixture<CreateConversationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CreateConversationComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(CreateConversationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});