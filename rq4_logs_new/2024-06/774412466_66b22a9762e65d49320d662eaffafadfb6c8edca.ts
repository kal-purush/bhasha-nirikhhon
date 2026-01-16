import { CommonModule } from '@angular/common';
import { Component, Input } from '@angular/core';
import { RouterModule } from '@angular/router';

@Component({
  selector: 'app-marketing-testimonials',
  standalone: true,
  imports: [RouterModule, CommonModule],
  templateUrl: './marketing-testimonials.component.html',
  styleUrl: './marketing-testimonials.component.scss',
})
export class MarketingTestimonialsComponent {
  constructor() {}

  @Input() testimonials: any = [];
}