import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { LayoutComponent } from './layout/layout.component';
import { ApplicationRoutes } from './application.routing';
import { FormsModule } from '@angular/forms';
import { MenubarModule } from 'primeng/menubar';

@NgModule({
  imports: [CommonModule, ApplicationRoutes, FormsModule, MenubarModule],
  declarations: [LayoutComponent],
})
export class ApplicationModule {}