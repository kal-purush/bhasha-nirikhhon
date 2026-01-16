import { Component, Input, OnInit } from '@angular/core';
import { ChatService } from '../../service/chat.service';
import { HttpClient } from '@angular/common/http';

@Component({
  selector: 'app-chat',
  templateUrl: './view-chat.component.html',
  styleUrl: './view-chat.component.css',
})
export class ViewChatComponent implements OnInit {
  userId: string = '';
  conversations: any[] = [];
  currentConversationId: string | null = null;
  messages: any[] = [];
  newMessage: string = '';

  constructor(private http: HttpClient, private socketService: ChatService) {}

  ngOnInit() {
    this.socketService.onNewMessage().subscribe((message) => {
      if (message.conversationId === this.currentConversationId) {
        this.messages.push(message);
      }
    });
  }

  setUserId(event: Event) {
    event.preventDefault();
    if (this.userId.trim()) {
      this.loadConversations();
    }
  }

  loadConversations() {
    this.http
      .get<any[]>(`http://localhost:3000/conversations/${this.userId}`)
      .subscribe(
        (conversations) => (this.conversations = conversations),
        (error) => console.error('Error al cargar las conversaciones:', error)
      );
  }

  joinConversation(conversationId: string) {
    this.currentConversationId = conversationId;
    this.messages = [];

    this.socketService.joinConversation(conversationId);

    this.http
      .get<any[]>(
        `http://localhost:3000/conversations/${conversationId}/messages`
      )
      .subscribe(
        (messages) => (this.messages = messages),
        (error) => console.error('Error al cargar los mensajes:', error)
      );
  }

  sendMessage(event: Event) {
    event.preventDefault();
    if (this.newMessage.trim() && this.currentConversationId) {
      const message = {
        content: this.newMessage,
        senderId: this.userId,
        conversationId: this.currentConversationId,
      };

      this.socketService.sendMessage(message);
      this.newMessage = '';
    }
  }
}