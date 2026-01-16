export class LocalizableContent_Mdl {
	content: any = {};

	private currentLocale: string;

	constructor(private baseContentObj: any,
	            private locale: string,
				private unregisterCallback: Function) {
		this.setLocale(locale);
	}

	setLocale(locale: string) {
		let src = this.baseContentObj[locale] ? this.baseContentObj[locale] : this.baseContentObj;
		let target = this.content;

		if(locale === this.currentLocale) {
			return;
		}

		this.currentLocale = locale;

		for(let key in target){
			if(target.hasOwnProperty(key)) {
				delete target[key];
			}
		}

		for(let key in src) {
			if(src.hasOwnProperty(key)) {
				target[key] = src[key];
			}
		}
	}

	unregister() {
		this.unregisterCallback(this);
	}
}