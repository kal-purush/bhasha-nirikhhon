import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ChatEtudiantComponent } from './chat-etudiant.component';

describe('ChatEtudiantComponent', () => {
  let component: ChatEtudiantComponent;
  let fixture: ComponentFixture<ChatEtudiantComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ChatEtudiantComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ChatEtudiantComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});