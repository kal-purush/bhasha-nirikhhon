/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { MeetupSuggestionService } from './meetup-suggestion.service';

describe('MeetupSuggestionService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [MeetupSuggestionService]
    });
  });

  it('should ...', inject([MeetupSuggestionService], (service: MeetupSuggestionService) => {
    expect(service).toBeTruthy();
  }));
});