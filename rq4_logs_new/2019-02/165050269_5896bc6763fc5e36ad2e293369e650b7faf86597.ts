import { TestBed } from '@angular/core/testing';

import { UsersRepositoryService } from './users-repository.service';
import {
  HttpClientTestingModule,
  HttpTestingController
} from '@angular/common/http/testing';
import { ApiRoute } from '@assets/constants';
import { usersFakeFactory } from '@modules/users/test-doubles';

describe('UsersRepositoryService', () => {
  let service: UsersRepositoryService;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [UsersRepositoryService]
    });

    service = TestBed.get(UsersRepositoryService);
    httpTestingController = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it('should call getAll correctly', () => {
    service.getAll().subscribe(() => {});
    httpTestingController.expectOne(`${ApiRoute.BASE}${ApiRoute.USERS}`);
  });
});