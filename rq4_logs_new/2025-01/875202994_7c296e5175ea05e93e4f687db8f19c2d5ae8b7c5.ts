import { Component, input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { CdkDragMove, DragDropModule } from '@angular/cdk/drag-drop';

@Component({
  selector: 'app-player-progress',
  standalone: true,
  imports: [CommonModule, DragDropModule],
  templateUrl: './player-progress.component.html',
})
export class PlayerProgressComponent {
  progress = input<{ progressMs: number; durationMs: number }>();

  positionX = 0;
  dragPosition: { x: number; y: number } = { x: 0, y: 0 };

  isMoving = false;
  isDragEnd = false;

  onDragMove(event: CdkDragMove): void {
    this.isMoving = true;
    this.isDragEnd = false;

    const positionX = this.positionX > 0 ? this.positionX + event.distance.x : event.distance.x;
    this.dragPosition = { x: positionX, y: 0 };
  }

  onDragEnd(): void {
    this.isMoving = false;
    this.isDragEnd = true;

    this.positionX = this.dragPosition.x;
  }

  onProgressLineClick(event: MouseEvent | Event): void {
    if (this.isDragEnd) {
      this.isDragEnd = false;
      return;
    }

    if (event instanceof MouseEvent) {
      this.dragPosition = { x: event.offsetX, y: 0 };
      this.positionX = this.dragPosition.x;
    }
  }
}