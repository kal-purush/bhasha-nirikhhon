import { Component, OnInit } from '@angular/core';
import { Thinking } from '../../interfaces/thinking';
import { ActivatedRoute, Router } from '@angular/router';
import { ThinkingService } from '../../services/thinking.service';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

@Component({
  selector: 'app-edit-thought',
  imports: [CommonModule, FormsModule],
  templateUrl: './edit-thought.component.html',
})
export class EditThoughtComponent implements OnInit {
  thinking: Thinking = {
    id: 0,
    auth: '',
    content: '',
    model: ''
  }

  constructor(
    private readonly thoughtService: ThinkingService,
    private readonly activatedRoute: ActivatedRoute,
    private readonly router: Router
  ) { }

  ngOnInit(): void {
    const id = this.activatedRoute.snapshot.params['id'];
    if (id) {
      this.thoughtService.findById(parseInt(id)).subscribe((thinking: Thinking) => {
        this.thinking = thinking;
      })
    }
  }
  editThink() {
    this.thoughtService.edit(this.thinking).subscribe(() => {
      this.router.navigate(['/']);
    });
  }
  cancel() {
    this.router.navigate(['/']);
  }
}