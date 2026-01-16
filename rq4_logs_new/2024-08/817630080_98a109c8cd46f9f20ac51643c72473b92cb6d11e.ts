import { ChangeDetectionStrategy, Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { CardComponent } from '../../features/card/card.component';

@Component({
  selector: 'app-footnote',
  standalone: true,
  imports: [CommonModule, CardComponent],
  templateUrl: './footnote.component.html',
  styleUrl: './footnote.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class FootnoteComponent {
  cards = [
    {
      title: 'Footnote 1',
      imageUrl: 'assets/images/tab-img-1.png',
      route: '/footnote/footnote1',
    },
  ];
}