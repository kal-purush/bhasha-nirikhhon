import { Injectable } from '@angular/core';
import { Storage } from '@ionic/storage';

@Injectable({
  providedIn: 'root'
})
export class ThemeService {

  public forceDark: boolean;

  constructor(
    private storage: Storage
  ) { }

  load() {
    this.storage.get("FORCE_DARK_THEME").then((val) => {
      this.forceDark = val == 'TRUE';
      document.body.classList.toggle('dark', this.forceDark);
    });
  }

  toggle() {
    var storedVal = this.forceDark ? 'TRUE' : 'FALSE';
    this.storage.set("FORCE_DARK_THEME", storedVal);
    document.body.classList.toggle('dark', this.forceDark);
  }
}