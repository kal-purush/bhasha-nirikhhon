import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DashboardCardPickerComponent } from './dashboard-card-picker.component';

describe('DashboardCardPickerComponent', () => {
    let component: DashboardCardPickerComponent;
    let fixture: ComponentFixture<DashboardCardPickerComponent>;

    beforeEach(async(() => {
        TestBed.configureTestingModule({
            declarations: [DashboardCardPickerComponent]
        })
            .compileComponents();
    }));

    beforeEach(() => {
        fixture = TestBed.createComponent(DashboardCardPickerComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});