import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-view-switcher',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './view-switcher.component.html',
  styleUrl: './view-switcher.component.scss',
})
export class ViewSwitcherComponent {
  @Input() currentView: 'preview' | 'code' | 'doc' = 'preview';
  @Input() title: string = 'View Switcher'; // Ajoutez une entrée pour le titre
  @Output() viewChange = new EventEmitter<'preview' | 'code' | 'doc'>();

  switchView(view: 'preview' | 'code' | 'doc'): void {
    this.viewChange.emit(view);
  }
}