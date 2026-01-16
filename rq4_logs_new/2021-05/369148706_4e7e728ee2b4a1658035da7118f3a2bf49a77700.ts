import { NgModule } from '@angular/core';

// Angular Material Imports
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatCardModule } from '@angular/material/card';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatPaginatorModule } from '@angular/material/paginator';

@NgModule({
  /**
   * We cand directly do exports only,
   * as angular manages imports for us
   * which is easy... 😉
   */

  // imports: [
  //   MatToolbarModule,
  //   MatCardModule,
  //   MatInputModule,
  //   MatButtonModule,
  //   MatExpansionModule,
  //   MatPaginatorModule,
  // ],

  exports: [
    MatToolbarModule,
    MatCardModule,
    MatInputModule,
    MatButtonModule,
    MatExpansionModule,
    MatPaginatorModule,
  ]
})

export class AngularMaterialModule { }