import { ComponentFixture, TestBed } from '@angular/core/testing';
import { EditThoughtComponent } from './edit-thought.component';
import { ThinkingService } from '../../services/thinking.service';
import { ActivatedRoute, Router } from '@angular/router';
import { of } from 'rxjs';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';

class MockThinkingService {
  findById = jasmine
    .createSpy('findById')
    .and.returnValue(
      of({ id: 1, auth: 'Autor', content: 'Conteúdo', model: 'model1' })
    );
  edit = jasmine.createSpy('edit').and.returnValue(of(null));
}

describe('EditThoughtComponent', () => {
  let component: EditThoughtComponent;
  let fixture: ComponentFixture<EditThoughtComponent>;
  let mockThinkingService: MockThinkingService;
  let mockRouter = { navigate: jasmine.createSpy('navigate') };

  beforeEach(async () => {
    mockThinkingService = new MockThinkingService();

    await TestBed.configureTestingModule({
      imports: [CommonModule, FormsModule, EditThoughtComponent],
      providers: [
        { provide: ThinkingService, useValue: mockThinkingService },
        { provide: Router, useValue: mockRouter },
        {
          provide: ActivatedRoute,
          useValue: { snapshot: { params: { id: '1' } } },
        },
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EditThoughtComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });
  it('must search a thought by ID at startup', () => {
    expect(mockThinkingService.findById).toHaveBeenCalledWith(1);
    expect(component.thinking).toEqual({ id: 1, auth: 'Autor', content: 'Conteúdo', model: 'model1' });
  });
});