import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { MatSnackBar } from '@angular/material';
import { By } from '@angular/platform-browser';
import { HttpErrorResponse } from '@angular/common/http';
import { Subject } from 'rxjs';

import { BatchSubmitComponent } from './batch-submit.component';
import { RecordService } from '../../services/record.service';


describe('BatchSubmitComponent', () => {
  let component: BatchSubmitComponent;
  let fixture: ComponentFixture<BatchSubmitComponent>;
  let batchSubmitRecord$: Subject<{'count': number}>;
  let snackBarOpen: jasmine.Spy;
  const tmpfile = new File([], 'a.xlsx');

  beforeEach(async(() => {
    snackBarOpen = jasmine.createSpy();
    batchSubmitRecord$ = new Subject();
    TestBed.configureTestingModule({
      declarations: [ BatchSubmitComponent ],
      imports: [
        HttpClientTestingModule,
      ],
      providers: [
        {
          provide: MatSnackBar,
          useValue: {
            open: snackBarOpen,
          },
        },
        {
          provide: RecordService,
          useValue: {
            batchSubmitRecord: () => batchSubmitRecord$,
          },
        },
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BatchSubmitComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should call onFileSlect when button is clicked.', () => {
    fixture.detectChanges();
    const fileInput = fixture.debugElement.query(By.css('input[type="file"]'));
    fileInput.triggerEventHandler('change', {
      target: {
        files: [tmpfile],
      }
    });

    expect(component.file).toBe(tmpfile);
    expect(component.flag).toBeFalsy();
  });

  it('should display errors when creation failed.', () => {
    component.uploadFile();
    batchSubmitRecord$.error({
      message: 'Raw error message',
      error: {
        detail: 'Invalid number of attachments',
      },
    } as HttpErrorResponse);

    expect(snackBarOpen).toHaveBeenCalledWith('Invalid number of attachments。', '关闭');
  });

  it('should display raw errors when creation failed.', () => {
    component.uploadFile();
    batchSubmitRecord$.error({
      message: 'Raw error message',
    } as HttpErrorResponse);

    expect(snackBarOpen).toHaveBeenCalledWith('Raw error message', '关闭');
  });

  it('should display count when creation successed.', () => {
    component.uploadFile();
    batchSubmitRecord$.next({
      count: 4
    } as {'count': number});

    expect(component.count).toBe(4);
    expect(component.flag).toBeTruthy();
  });
});
