import { Injectable } from '@angular/core';

@Injectable({
    providedIn: 'root'
})
export class StyleService {
    get transitionTime(): string {
        return this.getProperty('--transition-time');
    }

    get transitionTimeUnitless(): number {
        return this.getUnitlessValue(this.transitionTime);
    }

    private getProperty(property: string) {
        return getComputedStyle(document.documentElement).getPropertyValue(property);
    }

    private getUnitlessValue(value: string): number {
        const onlyNumbersRegExp: RegExp = /\D/g;
        return parseInt(value.replace(onlyNumbersRegExp, ''));
    }
}