import { Component, OnInit } from '@angular/core';
import { TranslateService } from '@ngx-translate/core';

@Component({
  selector: 'app-translation',
  templateUrl: './translation.component.html',
  styleUrls: ['./translation.component.css']
})
export class TranslationComponent implements OnInit {
  public activeLanguage = 'es';

  constructor(private translate: TranslateService) {
    this.translate.setDefaultLang(this.activeLanguage);
  }
  ngOnInit() {
  }

  public changeLanguage( lang) {
    this.activeLanguage = lang;
    this.translate.use(lang);
  }

}