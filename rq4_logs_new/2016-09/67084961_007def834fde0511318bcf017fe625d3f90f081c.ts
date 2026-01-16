import { Component, EventEmitter } from '@angular/core';
import { Session } from '../../shared/state/session';
import { Input, Output } from '@angular/core/src/metadata/directives';

@Component({
  selector: '[appSessionListItem]',
  templateUrl: './session-list-item.component.html',
  styleUrls: ['./session-list-item.component.css']
})
export class SessionListItemComponent {
  @Input() session: Session;
  @Output() sessionClick = new EventEmitter();
  @Output() toggleFavoriteClick = new EventEmitter();

  onSessionClick(id) {
    this.sessionClick.emit(id);
  }

  onToggleFavoriteClick(id) {
    this.toggleFavoriteClick.emit(id);
  }
}