import { ChangeDetectionStrategy, Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { CardComponent } from '../../features/card/card.component';

@Component({
  selector: 'app-show-more',
  standalone: true,
  imports: [CommonModule, CardComponent],
  templateUrl: './show-more.component.html',
  styleUrl: './show-more.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ShowMoreComponent {
  cards = [
    {
      title: 'show-more 1',
      imageUrl: 'assets/images/tab-img-1.png',
      route: '/show-more/show-more1',
    },
  ];
}