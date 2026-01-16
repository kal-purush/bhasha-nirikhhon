import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ThoughtComponent } from './thought.component';
import { ThinkingService } from '../../services/thinking.service';
import { ConfirmationDialogService } from '../shared/confirmation-dialog/confirmation-dialog.component';
import { of } from 'rxjs';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { FontAwesomeModule } from '@fortawesome/angular-fontawesome';
import { ActivatedRoute, RouterModule } from '@angular/router';
import { Thinking } from '../../interfaces/thinking';

describe('ThoughtComponent', () => {
  let component: ThoughtComponent;
  let fixture: ComponentFixture<ThoughtComponent>;
  let thinkingServiceSpy: jasmine.SpyObj<ThinkingService>;
  let confirmationDialogServiceSpy: jasmine.SpyObj<ConfirmationDialogService>;

  beforeEach(() => {
    thinkingServiceSpy = jasmine.createSpyObj('ThinkingService', ['delete', 'changeFavorite']);
    confirmationDialogServiceSpy = jasmine.createSpyObj('ConfirmationDialogService', ['openDialog']);

    TestBed.configureTestingModule({
      imports: [
        RouterModule,
        FontAwesomeModule,
        MatDialogModule,
        MatButtonModule,
        ThoughtComponent
      ],
      providers: [
        { provide: ThinkingService, useValue: thinkingServiceSpy },
        { provide: ConfirmationDialogService, useValue: confirmationDialogServiceSpy },
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: { paramMap: { get: jasmine.createSpy().and.returnValue('1') } } 
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    });

    fixture = TestBed.createComponent(ThoughtComponent);
    component = fixture.componentInstance;
  });
  it('should call the delete method and emit loadList when confirmation is true', () => {
    const thinkingId = 1;
    const event = new MouseEvent('click');
    confirmationDialogServiceSpy.openDialog.and.returnValue(of(true));
    thinkingServiceSpy.delete.and.returnValue(of({ id: 1, content: 'Sample content', auth: 'John Doe', model: 'XYZ' } as Thinking));

    spyOn(component.loadList, 'emit');

    component.delete(thinkingId, event);

    expect(confirmationDialogServiceSpy.openDialog).toHaveBeenCalled();
    expect(thinkingServiceSpy.delete).toHaveBeenCalledWith(thinkingId);
    expect(component.loadList.emit).toHaveBeenCalled();
  });
})