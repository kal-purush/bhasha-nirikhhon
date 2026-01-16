import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { StarRatingComponent } from '../../../shared/components/star-rating/star-rating.component';
import { CatalogCard } from 'src/app/core/interfaces/catalog';
import { TruncateDirective } from 'src/app/shared/directives/truncate.directive';

@Component({
  selector: 'catalog-horizontal-card',
  standalone: true,
  imports: [CommonModule, StarRatingComponent, TruncateDirective],
  templateUrl: './catalog-horizontal-card.component.html',
  styleUrls: ['./catalog-horizontal-card.component.scss']
})
export class CatalogHorizontalCardComponent {
  @Input() public card!: CatalogCard;
}