import { async, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Router } from '@angular/router';

import { ToastrService } from 'ngx-toastr';

import { AuthService } from '../auth/auth.service';

describe('AuthService', () => {
  let toastr: SpyObject<ToastrService>;
  let router: SpyObject<Router>;

  beforeEach(async(() => {
    toastr = createSpyObject(['error', 'success']);
    router = createSpyObject(['navigate']);

    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule
      ],
      providers: [
        { provide: ToastrService, useValue: toastr },
        { provide: Router, useValue: router },
        AuthService
      ]
    })
    .compileComponents();
  }));

  it('should be created', () => {
    const service: AuthService = TestBed.get(AuthService);
    expect(service).toBeTruthy();
  });
});