import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { UsersService } from './users.service';
import { IUser } from '../interfaces/user';

describe('UsersService', () => {
  let service: UsersService;
  let httpMock: HttpTestingController;
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule,
      ],
    });
    service = TestBed.get(UsersService);
    httpMock = TestBed.get(HttpTestingController);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should request http get method and return list of users', () => {
    const testData: IUser[] = [
      {
        name: 'user1',
        email: 'user1@email.com',
        address: {
          street: 'user1 street',
          suite: '1',
          city: 'user1 city',
          zipcode: '123',
          geo: {
            lat: 12,
            lng: 32,
          },
        },
        website: 'www.user1.com',
      },
      {
        name: 'user2',
        email: 'user2@email.com',
        address: {
          street: 'user2 street',
          suite: '2',
          city: 'user2 city',
          zipcode: '456',
          geo: {
            lat: 34,
            lng: 45,
          }
        },
        website: 'www.user2.com',
      },
    ];

    service.getUsers().subscribe(users => {
        expect(users.length).toBe(2);
        expect(users).toEqual(testData);
    });
    const request = httpMock.expectOne( `${service.userAPI}`);
    expect(request.request.method).toBe('GET');
    request.flush(testData);
  });

  afterEach(() => {
    // make sure there is no request outstanding
    httpMock.verify();
  });
});