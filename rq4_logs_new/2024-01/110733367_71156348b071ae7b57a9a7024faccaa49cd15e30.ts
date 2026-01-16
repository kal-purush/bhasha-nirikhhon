import { CommonModule } from '@angular/common';
import { Component, Input } from '@angular/core';

@Component({
  selector: 'app-profile-content',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './profile-content.component.html',
  styleUrl: './profile-content.component.scss',
})
export class ProfileContentComponent {
  @Input()
  public location: string;

  @Input()
  public year: string;

  @Input()
  public contents: Array<string>;

  @Input()
  public closeSection?: boolean = false;
}