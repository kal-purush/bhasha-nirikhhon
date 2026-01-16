import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ThinkingService } from '../../../services/thinking.service';
import { Router } from '@angular/router';
import { Thinking } from '../../../interfaces/thinking';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-thought-form',
  imports: [CommonModule, FormsModule],
  templateUrl: './thought-form.component.html',
})
export class ThoughtFormComponent {
  thinking: Thinking = {
    content: '',
    auth: '',
    model: 'model1'
  }

  constructor(
    private readonly thinkingService: ThinkingService,
    private readonly router: Router) { }

  createThink() {
    this.thinkingService.create(this.thinking).subscribe(() => {
      this.router.navigate(['/listarPensamento']);
    });
  }
  cancel() {
    this.router.navigate(['/listarPensamento']);
  }
}