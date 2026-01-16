import { Component, OnInit } from '@angular/core';
import { Thinking } from '../../interfaces/thinking';
import { ThinkingService } from '../../services/thinking.service';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { FontAwesomeModule } from '@fortawesome/angular-fontawesome';
import { faCommentSlash } from '@fortawesome/free-solid-svg-icons';
import { ThoughtComponent } from '../../components/thought/thought/thought.component';

@Component({
  selector: 'app-list-thought',
  imports: [CommonModule, ThoughtComponent, RouterModule, FontAwesomeModule],
  templateUrl: './list-thought.component.html',
})
export class ListThoughtComponent implements OnInit {
  listThounght: Thinking[] = []
  constructor(private readonly service: ThinkingService) { }
  commentSplash = faCommentSlash
  ngOnInit(): void {
    this.service.list().subscribe((listThounght) => {
      this.listThounght = listThounght
    })
  }
}