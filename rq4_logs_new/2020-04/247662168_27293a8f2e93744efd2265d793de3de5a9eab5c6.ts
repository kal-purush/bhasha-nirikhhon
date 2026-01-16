import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { UsersService } from './users.service';
import { HttpClient } from '@angular/common/http';
import { UserModel } from './user.model';

describe('UsersService', () => {
  let service: UsersService;
  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  let user: UserModel = {
    id: '1',
    firstName: 'Prashant',
    lastName: 'Choudhary',
    login: 'pkc',
    password: 'pkc@123',
    isDeleted: false,
    age: 20
  };

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule]
    });
    service = TestBed.inject(UsersService);

    // Inject the http service and test controller for each test
    httpClient = TestBed.inject(HttpClient);
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  it('should get all users', ()=>{
    service.getUsers().subscribe((data)=>{
      expect(data).toEqual([]);
    });
    const req = httpTestingController.expectOne('http://localhost:8080/users/');
    expect(req.request.method).toEqual('GET');
    req.flush([]);
    httpTestingController.verify();
  });

  it('should change status of user', ()=>{
    service.changeStatus(user).subscribe((data)=>{
      expect(data).toEqual(user);
    });
    const req = httpTestingController.expectOne(`http://localhost:8080/users/${user.id}`);
    expect(req.request.method).toEqual('PUT');
    req.flush(user);
    httpTestingController.verify();
  });

  it('should get user', ()=>{
    service.getUser(user.id).subscribe((data)=>{
      expect(data).toEqual(user);
    });
    const req = httpTestingController.expectOne(`http://localhost:8080/users/${user.id}`);
    expect(req.request.method).toEqual('GET');
    req.flush(user);
    httpTestingController.verify();
  });

  it('should create user', ()=>{
    service.createUser(user).subscribe((data)=>{
      expect(data).toEqual(user);
    });
    const req = httpTestingController.expectOne(`http://localhost:8080/users/`);
    expect(req.request.method).toEqual('POST');
    req.flush(user);
    httpTestingController.verify();
  });

  it('should update user', ()=>{
    service.updateUser(user).subscribe((data)=>{
      expect(data).toEqual(user);
    });
    const req = httpTestingController.expectOne(`http://localhost:8080/users/${user.id}`);
    expect(req.request.method).toEqual('PUT');
    req.flush(user);
    httpTestingController.verify();
  });

  it('should get active users', ()=>{
    service.getActiveUsers().subscribe((data)=>{
      expect(data).toEqual([]);
    });
    const req = httpTestingController.expectOne(`http://localhost:8080/users/`);
    expect(req.request.method).toEqual('GET');
    req.flush([]);
    httpTestingController.verify();
  });

  it('should get inactive users', ()=>{
    service.getInActiveUsers().subscribe((data)=>{
      expect(data).toEqual([]);
    });
    const req = httpTestingController.expectOne(`http://localhost:8080/users/`);
    expect(req.request.method).toEqual('GET');
    req.flush([]);
    httpTestingController.verify();
  });

});