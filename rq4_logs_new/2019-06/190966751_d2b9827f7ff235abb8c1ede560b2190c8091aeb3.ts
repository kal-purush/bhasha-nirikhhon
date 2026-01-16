// This class keeps track of settings variables (and should be accessed to retreive those values).
// It also stores settings values in storage

import { Injectable, OnInit } from '@angular/core';
import { StorageService } from './storage.service';
import { ThemeService } from './theme.service';

@Injectable({
  providedIn: 'root'
})
export class SettingsService implements OnInit {
  // True = dark mode on
  isDark: boolean;

  constructor(private storageService: StorageService,
              private themeService: ThemeService) {
              }

  async ngOnInit() {
    await this.applySettingsFromStorage();
  }

  setDarkMode(isDark: boolean) {
    this.storageService.store('isDark', isDark.toString());

    if (isDark) {
      this.themeService.setTheme('dark');
    } else {
      this.themeService.setTheme('none');
    }
  }

  // Retrieves settings from storage and applies the,
  async applySettingsFromStorage() {
    // Dark mode
    this.isDark = JSON.parse(await this.storageService.retrieve('isDark'));
    this.setDarkMode(this.isDark);
  }
}