import { Component, Input, inject } from '@angular/core';
import { MatIcon } from '@angular/material/icon';
import { Router } from '@angular/router';

@Component({
  selector: 'app-menu-item',
  templateUrl: './menu-item.component.html',
  styleUrls: ['../layout.component.scss', './menu-item.component.scss'],
  imports: [MatIcon],
})
export class MenuItemComponent {
  @Input() title!: string;
  @Input() icon?: string;
  @Input() redirectUrl!: string;

  private router = inject(Router);
  get isActive(): boolean {
    return this.router.url === '/' + this.redirectUrl;
  }
  redirect() {
    this.router.navigate([this.redirectUrl]);
  }
}