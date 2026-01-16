import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { HeaderComponent } from './header.component';

const components = [
  HeaderComponent
];

@NgModule({
  imports: [CommonModule],
  providers: [],
  declarations: components,
  exports: [...components, CommonModule]
})
export class HeaderModule {}