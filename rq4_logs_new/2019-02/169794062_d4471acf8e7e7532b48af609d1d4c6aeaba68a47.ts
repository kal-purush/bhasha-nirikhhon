import { NgModule } from '@angular/core';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import {
	MatButtonModule,
	MatCheckboxModule,
	MatToolbarModule,
	MatSidenavModule,
	MatRippleModule,
	MatMenuModule,
	MatCardModule,
	MatButtonToggleModule
} from '@angular/material';

@NgModule({
	imports: [
		BrowserAnimationsModule,

		MatButtonModule,
		MatCheckboxModule,
		MatToolbarModule,
		MatSidenavModule,
		MatRippleModule,
		MatMenuModule,
		MatCardModule,
		MatButtonToggleModule
	],
	exports: [
		MatButtonModule,
		MatCheckboxModule,
		MatToolbarModule,
		MatSidenavModule,
		MatRippleModule,
		MatMenuModule,
		MatCardModule,
		MatButtonToggleModule
	],
})
export class AppMaterialModule { }