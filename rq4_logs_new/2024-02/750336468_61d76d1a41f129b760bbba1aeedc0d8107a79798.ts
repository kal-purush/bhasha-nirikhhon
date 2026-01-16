import { PublicLayoutComponent } from './public-layout/public-layout.component';
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { PublicRoutingModule } from './public-routing.module';
import { ServicesComponent } from './pages/services/services.component';
import { SharedModule } from '../shared/shared.module';
import { CardComponent } from '../components/card/card.component';
import { LoaderComponent } from '../components/loader/loader.component';
import { PaperComponent } from '../components/paper/paper.component';
import { SectionComponent } from '../components/section/section.component';
import { HomeComponent } from './pages/home/home.component';

@NgModule({
  declarations: [ServicesComponent, PublicLayoutComponent, HomeComponent],
  imports: [
    CommonModule,
    PublicRoutingModule,
    SharedModule,
    PaperComponent,
    SectionComponent,
    CardComponent,
    LoaderComponent,
  ],
})
export class PublicModule {}