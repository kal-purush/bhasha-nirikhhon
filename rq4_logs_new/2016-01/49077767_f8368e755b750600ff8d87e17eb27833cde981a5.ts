import {
  describe,
  it,
  expect
} from 'angular2/testing';
import {Observable} from 'rxjs/Observable';
import {Meetup} from './meetup-model';
import {MeetupListComponent} from './meetup-list.component';


describe('meetup list component success', function () {
  var component, mockMeetupProvider, meetupList;

  class MockParameters {
    get(param: string) {
      return param;
    }
  }

  beforeEach(() => {
    mockMeetupProvider = jasmine.createSpyObj('mockMeetupProvider', ['getList']);
    meetupList = [new Meetup('1', 'meetup 1', 'cool meetup', 'meetup.example.com', new Date())];
    mockMeetupProvider.getList.and.returnValue(
      Observable.create(function (subscriber) {
        subscriber._next(meetupList);
        subscriber._complete();
      })
    );

    var mockParameters = new MockParameters();
    component = new MeetupListComponent(mockParameters, mockMeetupProvider);
  });

  it('should find meetup list', () => {
    expect(component.meetupList).toEqual(meetupList);
    expect(mockMeetupProvider.getList).toHaveBeenCalledWith('city', 'countryCode');
  });

});


describe('empty meetup list component', function () {
  var component, mockMeetupProvider, emptyMeetupList;

  class MockParameters {
    get(param: string) {
      return param;
    }
  }

  beforeEach(() => {
    mockMeetupProvider = jasmine.createSpyObj('mockMeetupProvider', ['getList']);
    emptyMeetupList = [];
    mockMeetupProvider.getList.and.returnValue(
      Observable.create(function (subscriber) {
        subscriber._error('ERROR: THIS WILL BE LOGGED!');
      })
    );

    var mockParameters = new MockParameters();
    component = new MeetupListComponent(mockParameters, mockMeetupProvider);
  });


  it('should return an empty meetup list', () => {
    expect(component.meetupList).toEqual(emptyMeetupList);
    expect(mockMeetupProvider.getList).toHaveBeenCalledWith('city', 'countryCode');
  });

});