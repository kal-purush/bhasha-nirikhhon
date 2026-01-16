import { async, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { ToastrService } from 'ngx-toastr';

import { AuthService } from '../auth/auth.service';
import { GroupService } from '../groups/group.service';
import { UserService } from './user.service';

describe('UserService', () => {
  let toastr: SpyObject<ToastrService>;
  let authService: SpyObject<AuthService>;
  let groupService: SpyObject<GroupService>;

  beforeEach(async(() => {
    toastr = createSpyObject(['error', 'success']);
    authService = createSpyObject(['getUserId', 'getUserName']);
    groupService = createSpyObject(['getCurrentGroup']);

    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule
      ],
      providers: [
        { provide: AuthService, useValue: authService },
        { provide: GroupService, useValue: groupService },
        { provide: ToastrService, useValue: toastr },
        UserService
      ]
    })
    .compileComponents();
  }));

  it('should be created', () => {
    const service: UserService = TestBed.get(UserService);
    expect(service).toBeTruthy();
  });
});