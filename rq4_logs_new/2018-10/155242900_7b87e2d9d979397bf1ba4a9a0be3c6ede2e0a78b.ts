import {MatButtonModule, MatCheckboxModule, MatIconModule, MatToolbarModule, MatFormFieldModule, MatOptionModule, MatSelectModule, MatInputModule, } from '@angular/material';
import { NgModule } from '@angular/core';

@NgModule({
    imports: [MatButtonModule, MatCheckboxModule, MatIconModule, MatToolbarModule, MatFormFieldModule, MatOptionModule, MatSelectModule, MatInputModule],

    exports: [MatButtonModule, MatCheckboxModule, MatIconModule, MatToolbarModule, MatFormFieldModule, MatOptionModule, MatSelectModule, MatInputModule],
})

export class MaterialModule { }