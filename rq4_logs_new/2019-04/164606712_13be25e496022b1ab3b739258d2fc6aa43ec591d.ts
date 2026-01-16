import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { Subject } from 'rxjs';

import { ProgramCategoryService } from './program-category.service';
import { AUTH_SERVICE } from 'src/app/shared/interfaces/auth-service';
import { environment } from 'src/environments/environment';

describe('ProgramCategoryService', () => {
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    const authenticationSucceed$ = new Subject<void>();
    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule,
      ],
      providers: [
        {
          provide: AUTH_SERVICE,
          useValue: {
            authenticationSucceed: authenticationSucceed$,
            isAuthenticated: true,
          },
        },
      ]
    });
    httpTestingController = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it('should be created', () => {
    const service: ProgramCategoryService = TestBed.get(ProgramCategoryService);
    expect(service).toBeTruthy();
  });
  it('should get program-category', () => {
    const service: ProgramCategoryService = TestBed.get(ProgramCategoryService);

    service.getProgramCategories({}).subscribe();

    const url = `${environment.API_URL}/program-categories/?limit=10&offset=0`;

    const req = httpTestingController.expectOne(url);
    expect(req.request.method).toEqual('GET');
    req.flush({});
  });
});