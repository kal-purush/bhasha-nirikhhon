import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ListThoughtComponent } from './list-thought.component';
import { ThinkingService } from '../../services/thinking.service';
import { of } from 'rxjs';
import { FontAwesomeModule } from '@fortawesome/angular-fontawesome';
import { provideRouter } from '@angular/router';
import { ThoughtComponent } from '../../components/thought/thought/thought.component';
import { CommonModule } from '@angular/common';

const mockThinkingService = {
  list: jasmine.createSpy('list').and.returnValue(of([
    {
      id: 1, content: 'Pensamento 1', auth: 'Lore',
      model: 'model1'
    },
    {
      id: 2, content: 'Pensamento 2', auth: 'Luan',
      model: 'model2'
    }
  ]))
};

describe('ListThoughtComponents', () => {
  let component: ListThoughtComponent;
  let fixture: ComponentFixture<ListThoughtComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CommonModule, FontAwesomeModule, ListThoughtComponent, ThoughtComponent],
      providers: [
        { provide: ThinkingService, useValue: mockThinkingService },
        provideRouter([])
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(ListThoughtComponent);
    component = fixture.componentInstance;
  });

  it('should call list() on ngOnInit and populate listThought', () => {
    component.ngOnInit();
    expect(mockThinkingService.list).toHaveBeenCalled();
    expect(component.listThounght.length).toBe(2);
  });

})