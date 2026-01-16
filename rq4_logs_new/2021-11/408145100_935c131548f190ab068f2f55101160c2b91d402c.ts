import { ComponentFixture, TestBed } from '@angular/core/testing';
import { AppComponent } from 'src/app/app.component';
import { NoticesComponent } from 'src/app/components/notices/notices.component';
import { LiturgicalDates } from 'src/app/models/liturgical-dates';
import { AppConfigService } from 'src/app/services/app-config.service';
import { AppDateService } from 'src/app/services/app-date.service';
import { LocalizationService } from 'src/app/services/localization.service';
import { HolyRosaryHomeComponent } from './holy-rosary-home.component';
import { MysterySelectorComponent } from './mystery-selector/mystery-selector.component';

describe('HomeComponent', () => {
  let component: HolyRosaryHomeComponent;
  let fixture: ComponentFixture<HolyRosaryHomeComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [
        HolyRosaryHomeComponent,
        MysterySelectorComponent,
        NoticesComponent
      ],
      providers: [
        { provide: AppDateService, useValue: new AppDateService(undefined) },
        LocalizationService,
        AppConfigService,
        AppComponent
      ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HolyRosaryHomeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});