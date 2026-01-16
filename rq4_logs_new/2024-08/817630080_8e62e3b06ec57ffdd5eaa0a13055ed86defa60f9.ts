import { ChangeDetectionStrategy, Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { CardComponent } from '../../features/card/card.component';
import { Modal1Component } from './modal1/modal1.component';

@Component({
  selector: 'app-modal',
  standalone: true,
  imports: [CommonModule, CardComponent],
  templateUrl: './modal.component.html',
  styleUrl: './modal.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ModalComponent {
  cards = [
    {
      title: 'Modal 1',
      imageUrl: 'assets/images/modal.png',
      route: '/modal/modal1',
    },
  ];
}