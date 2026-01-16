import { Component } from '@angular/core';
import { Config_Svc, Localization_Svc } from "../../";

@Component({
	selector: 'locale-switcher',
	template: require('./locale-switcher.cmp.html'),
	styles: [require('./locale-switcher.cmp.scss')]
})
export class LocaleSwitcher_Cmp {
	locale: any;
	locales: Array<any> = [];
	active: boolean = false;

	constructor(private configSvc: Config_Svc,
				private locSvc: Localization_Svc) {
		this.locale = locSvc.getCurrentLocale();
		this.locSvc.localeUpdatedEvent.subscribe(locale => {
			this.locale = locale;
			this.locales = this.locSvc.getLocales();
		});
	}

	toggle() {
		this.active = !this.active;
	}

	handleLocaleClick(locale:any) {
		this.locSvc.setLocale(locale.locale);
	}
}