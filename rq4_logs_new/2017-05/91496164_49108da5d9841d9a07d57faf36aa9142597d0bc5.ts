import { Component, Output, EventEmitter } from '@angular/core';

@Component({
  selector: 'logout-button',
  templateUrl: './logout-button.component.html',
  styleUrls: ['./logout-button.component.scss']
})
export class LogoutButtonComponent {

    @Output() onLogout = new EventEmitter();

    private logout() {
        this.onLogout.emit(true);
    }
}