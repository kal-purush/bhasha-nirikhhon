import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { NgxPaginationModule } from 'ngx-pagination';
import { CarsRoutingModule } from './../cars/cars-routing.module';

import { FilterComponent } from './filter/filter.component';
import { AdvancedSearchComponent } from './advanced-search.component';
import { FilterService } from './filter/filter.service';

@NgModule({
    imports: [
        CommonModule,
        CarsRoutingModule,
        FormsModule,
        ReactiveFormsModule,
        NgxPaginationModule
    ],
    declarations: [
        FilterComponent,
        AdvancedSearchComponent,
    ],
    providers: [
        FilterService
    ]
})

export class AdvancedSearchModule { }