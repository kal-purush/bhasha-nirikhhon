import { DOCUMENT } from '@angular/common';
import { Inject, Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class ThemeService {

  private theme: string = "light";
  public readonly THEME_KEY = "THEME";

  constructor(
    @Inject(DOCUMENT) private document: Document
  ) { }

  public initTheme(): void {
    const theme = localStorage.getItem(this.THEME_KEY) as "light"|"dark"|null;
    if(theme){
      this.setTheme(theme);
    } else {
      this.setTheme("light");
    }
  }

  public get currentTheme(): string {
    return this.theme;
  }

  public setTheme(theme: "light"|"dark"): void {
    localStorage.setItem(this.THEME_KEY, theme);
    this.theme = theme;

    const themeLink = this.document.getElementById("app-theme") as HTMLLinkElement;
    if(themeLink) {
        themeLink.href = `${theme}.css`;
    }
  }

}