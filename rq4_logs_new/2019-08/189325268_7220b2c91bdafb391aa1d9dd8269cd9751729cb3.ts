import { async, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { ToastrService } from 'ngx-toastr';

import { AuthService } from '../auth/auth.service';
import { GameService } from './game.service';
import { GroupService } from '../groups/group.service';

describe('GameService', () => {
  let toastr: SpyObject<ToastrService>;
  let authService: SpyObject<AuthService>;
  let groupService: SpyObject<GroupService>;

  beforeEach(async(() => {
    groupService = createSpyObject(['getCurrentGroup']);
    authService = createSpyObject(['getUserId', 'getUserName']);
    toastr = createSpyObject(['error', 'success']);

    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule
      ],
      providers: [
        { provide: ToastrService, useValue: toastr },
        { provide: AuthService, useValue: authService },
        GameService
      ]
    })
    .compileComponents();
  }));

  it('should be created', () => {
    const service: GameService = TestBed.get(GameService);
    expect(service).toBeTruthy();
  });
});