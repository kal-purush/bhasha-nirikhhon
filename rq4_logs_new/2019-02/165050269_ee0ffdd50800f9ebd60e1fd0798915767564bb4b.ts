import { TestBed } from '@angular/core/testing';
import {
  HttpClientTestingModule,
  HttpTestingController
} from '@angular/common/http/testing';

import { userFakeFactory, usersFakeFactory } from '@modules/users/test-doubles';
import { User } from '@modules/users/models';
import { UsersService } from '@modules/users/services';

describe('UsersService', () => {
  let service: UsersService;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [UsersService]
    });

    service = TestBed.get(UsersService);
    httpTestingController = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call fetchUsers correctly', () => {
    service.fetchUsers();
    httpTestingController
      .expectOne('https://reqres.in/api/users')
      .flush(usersFakeFactory());
    expect(service.state.users.length).toBe(3);
  });

  it('should set selected user', () => {
    const user: User = userFakeFactory();
    service.selectUser(user);
    expect(service.state.selectedUser).toBe(user);
  });
});