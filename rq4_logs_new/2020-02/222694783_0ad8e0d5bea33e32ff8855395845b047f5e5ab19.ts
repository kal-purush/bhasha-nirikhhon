import {async, ComponentFixture, TestBed} from '@angular/core/testing';
import {FormsModule} from '@angular/forms';
import {ScheduleAppComponent} from './scheduleApp.component';
import {ScheduleAppService} from './scheduleApp.service';
import {RouterTestingModule} from '@angular/router/testing';
import {of} from 'rxjs';
import {HttpClient, HttpClientModule, HttpHandler} from '@angular/common/http';
import {AppointmentType} from '../../models/appointmentType.model';
import {HttpClientTestingModule} from '@angular/common/http/testing';
import {By} from '@angular/platform-browser';

describe('ScheduleAppComponentIntegration', () => {
    let component: ScheduleAppComponent;
    let fixture: ComponentFixture<ScheduleAppComponent>;
    let scheduleService;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [ScheduleAppComponent],
            imports: [
                HttpClientModule,
                RouterTestingModule,
                HttpClientTestingModule,
                FormsModule
            ],
            providers: [
                ScheduleAppService
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ScheduleAppComponent);
        scheduleService = TestBed.get(ScheduleAppService);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeDefined();
    });

    it('when nothing is selected, no search is performed.', () => {
        const searchBtn = fixture.debugElement.query(By.css('#searchBtn')).nativeElement;
        searchBtn.click();
        expect(component.selectedType).toBeUndefined();
    });


});