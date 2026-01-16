import { ComponentFixture, TestBed } from '@angular/core/testing';

import { WorkerInputDialogComponent } from './worker-input-dialog.component';

describe('WorkerInputDialogComponent', () => {
    let component: WorkerInputDialogComponent;
    let fixture: ComponentFixture<WorkerInputDialogComponent>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            declarations: [WorkerInputDialogComponent],
        }).compileComponents();

        fixture = TestBed.createComponent(WorkerInputDialogComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});