import { Component } from '@angular/core';
import { Q1Component } from '../q1/q1.component';
import { Q2Component } from '../q2/q2.component';
import { Q3Component } from '../q3/q3.component';


@Component({
  selector: 'app-chart-page',
  standalone: true,
  imports: [
    Q1Component,
    Q2Component,
    Q3Component

  ], // La page servant juste à afficher les enfants, elle ne gère que leur import et chaque enfant importera le module de chart dont il a besoin
  templateUrl: './chart-page.component.html',
})
export class ChartPageComponent {
  data = jsonData as DataFromJson; // On cast le JSON en DataFromJson pour éviter les erreurs de type
}