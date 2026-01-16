import { NgModule } from '@angular/core';
import { MatTableModule,
         MatCardModule,
         MatSidenavModule,
         MatListModule,
         MatIconModule } from '@angular/material';
@NgModule ({
    imports: [
        MatTableModule,
        MatCardModule,
        MatSidenavModule,
        MatListModule,
        MatIconModule
    ],
    exports: [
        MatTableModule,
        MatCardModule,
        MatSidenavModule,
        MatListModule,
        MatIconModule
    ]
})
export class MaterialModule { }