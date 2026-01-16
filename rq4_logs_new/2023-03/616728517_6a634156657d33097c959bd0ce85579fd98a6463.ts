import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { IonicModule } from '@ionic/angular';
import { FormsModule } from '@angular/forms';
import { StepsHeaderComponent } from './steps-header/steps-header.component';
import { BookCollectionComponent } from './book-collection/book-collection.component';
import { BookListComponent } from './book-list/book-list.component';

@NgModule({
  imports: [CommonModule, FormsModule, IonicModule],
  declarations: [StepsHeaderComponent, BookCollectionComponent, BookListComponent],
  exports: [StepsHeaderComponent, BookCollectionComponent, BookListComponent],
})
export class ComponentsModule {}