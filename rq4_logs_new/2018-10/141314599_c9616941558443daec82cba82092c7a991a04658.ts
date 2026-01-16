import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { BrowserModule } from '@angular/platform-browser';
import { CommonModule } from '@angular/common';
import { BusySpinnerModule } from '../../widget/busy-spinner/busy-spinner.module';
import { CommercialSettlementDIComponent } from '../commercial-settlement-di/components/commercial-settlement-di.component';

@NgModule({
    imports:[
        ReactiveFormsModule,
        BrowserModule,
        CommonModule,
        BusySpinnerModule
    ],
    declarations:[
        CommercialSettlementDIComponent
    ],
    exports:[
        CommercialSettlementDIComponent
    ]
})

export class CommercialSettlementDIModule {}