import { Component, Input } from '@angular/core';
import { Thinking } from '../../../interfaces/thinking';
import { RouterModule } from '@angular/router';
import { CommonModule } from '@angular/common';
import { FontAwesomeModule } from '@fortawesome/angular-fontawesome';
import { faEdit, faTrash } from '@fortawesome/free-solid-svg-icons';

@Component({
  selector: 'app-thought',
  standalone: true,
  imports: [RouterModule, CommonModule, FontAwesomeModule],
  templateUrl: './thought.component.html',
})
export class ThoughtComponent {
  @Input() thinking: Thinking = {
    id: 0,
    content: 'I love Angular',
    auth: 'Luan',
    model: 'model1'
  };

  icons = { edit: faEdit, trash: faTrash };

  get boxShadow(): string {
    const shadowColors: Record<string, string> = {
      model1: '#154580',
      model2: '#6BD1FF',
      model3: '#84EEC1'
    };
    return shadowColors[this.thinking.model] ? `8px 8px 0px ${shadowColors[this.thinking.model]}` : 'none';
  }

  get thinkingClass(): string {
    return this.thinking.content.length >= 256 ? 'max-w-full' : 'max-w-[50%]';
  }
}