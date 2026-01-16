import { CommonModule } from '@angular/common';
import { Component, ElementRef, ViewChild } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Message } from '../../model/message';

@Component({
  selector: 'app-conversation',
  standalone: true,
  imports: [FormsModule, CommonModule],
  templateUrl: './conversation.component.html',
  styleUrl: './conversation.component.css'
})
export class ConversationComponent {
  messages: Message[] = [];
  message: string = '';
  maxHeight: number = 150;
  curUserID: number = 1;
  destUserID: number = 2;

  @ViewChild('messageBox') textArea !: ElementRef

  handleKeydown(event: KeyboardEvent): void {
    const textarea = this.textArea.nativeElement;

    if (event.key === 'Enter' && event.shiftKey) {
      this.adjustHeight(textarea);
    }
    else if (event.key === 'Enter') {
      event.preventDefault();
      const text = textarea.value.trim();
      if (text !== "" && text !== textarea.placeholder)
        this.sendMessage()
    }
  }

  adjustHeight(textarea: HTMLTextAreaElement): void {
    textarea.style.height = 'auto'

    if (textarea.scrollHeight <= this.maxHeight) {
      textarea.style.height = `${textarea.scrollHeight}px`;
    } else {
      textarea.style.height = `${this.maxHeight}px`;
      textarea.style.overflowY = 'auto';
    }
  }

  sendMessage(): void {
    this.messages.push(new Message(this.message, this.curUserID, this.destUserID))
    this.textArea.nativeElement.style.height = '50px';
    this.textArea.nativeElement.value = ''
  }


  uploadFile(): void {
    const fileInput = document.querySelector('input') as HTMLInputElement;
    fileInput.click();
  }

  handleFileInput(event: Event): void {
    const files = (event.target as HTMLInputElement).files;
    if (files && files.length > 0) {
      const file = files[0];
      const attachment = `Attachment : ${file.name}`;

    }
  }
}