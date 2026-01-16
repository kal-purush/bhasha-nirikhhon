import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ModalComponent } from '../modal/modal.component';

@Component({
  selector: 'app-card',
  standalone: true,
  imports: [CommonModule, ModalComponent],
  templateUrl: './card.component.html',
  styleUrl: './card.component.scss',
})
export class CardComponent {
  @Input() title: string = 'Titre de la carte';
  @Input() link: string = '/home';
  @Input() imageUrl: string = 'https://via.placeholder.com/300x200';
}