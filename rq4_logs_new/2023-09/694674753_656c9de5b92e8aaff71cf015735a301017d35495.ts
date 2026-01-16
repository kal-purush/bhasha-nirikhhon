import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { TranslateService } from '@ngx-translate/core';
import { DataStorageService } from 'src/app/core/services/data-storage.service';
import { AppConfigService } from 'src/app/app-config.service';
import { AuditService } from 'src/app/core/services/audit.service';
import { BreakpointService } from 'src/app/core/services/breakpoint.service';
import { LoginRedirectService } from 'src/app/core/services/loginredirect.service';
import { DashboardComponent } from './dashboard.component';
import { of } from 'rxjs';

describe('DashboardComponent', () => {
  let component: DashboardComponent;
  let fixture: ComponentFixture<DashboardComponent>;
  let router: Router;
  let translateService: TranslateService;
  let appConfigService: AppConfigService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DashboardComponent],
      providers: [
        {
          provide: Router,
          useValue: {
            navigate: jasmine.createSpy('navigate'),
          },
        },
        {
          provide: TranslateService,
          useValue: {
            use: jasmine.createSpy('use'),
            getTranslation: () => of({ menuItems: [] }), // Mock translation data
          },
        },
        {
          provide: AppConfigService,
          useValue: {
            getConfig: () => ({}), // Mock AppConfigService
          },
        },
        // Provide other services as needed
      ],
    });

    fixture = TestBed.createComponent(DashboardComponent);
    component = fixture.componentInstance;
    router = TestBed.get(Router);
    translateService = TestBed.get(TranslateService);
    appConfigService = TestBed.get(AppConfigService);
  });

  it('should create the DashboardComponent', () => {
    expect(component).toBeTruthy();
  });

  it('should navigate to "regcenter" when "Get Information" is selected', () => {
    const navigateSpy = spyOn(router, 'navigate');
    component.onItemSelected('Get Information');
    expect(navigateSpy).toHaveBeenCalledWith(['regcenter']);
  });

  it('should open a new window when "Booking an Appointment" is selected', () => {
    const openSpy = spyOn(window, 'open');
    const getConfigSpy = spyOn(appConfigService, 'getConfig').and.returnValue({
      'mosip-prereg-ui-url': 'https://example.com',
    });
    component.onItemSelected('Booking an Appointment');
    expect(openSpy).toHaveBeenCalledWith('https://example.com#/en', '_blank');
    expect(getConfigSpy).toHaveBeenCalled();
  });

  // Add more test cases as needed for other scenarios
});