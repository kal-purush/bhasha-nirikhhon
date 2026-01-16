import { TestBed, waitForAsync } from '@angular/core/testing';
import { WelcomeComponent } from './welcome.component';
import { UserService } from '../user/user.service';
import { Observable, of } from 'rxjs';

class MockUserService {
  user$: Observable<any | null> = of(null);
  logout() {}
}

describe('WelcomeComponent', () => {
  let userService: MockUserService;
  beforeEach(waitForAsync(() => {
    userService = new MockUserService();
    TestBed.configureTestingModule({
      imports: [WelcomeComponent],
      providers: [
        { provide: UserService, useValue: userService },
      ],
    }).compileComponents();
  }));

  it('renders the component', () => {
    const fixture = TestBed.createComponent(WelcomeComponent);
    const component = fixture.componentInstance;
    expect(component).toBeTruthy();
  });

  it('displays the user email', () => {    
    userService.user$ = of({ email: 'email' });
    const fixture = TestBed.createComponent(WelcomeComponent);
    const component = fixture.componentInstance;
    component.username$.subscribe(username => {
      expect(username).toBe('email');
    });
  });

  it('displays the null user', () => {    
    userService.user$ = of(null);
    const fixture = TestBed.createComponent(WelcomeComponent);
    const component = fixture.componentInstance;
    component.username$.subscribe(username => {
      expect(username).toBe('Anonymous');
    });
  });

  it('logs out the user', () => {
    const fixture = TestBed.createComponent(WelcomeComponent);
    const component = fixture.componentInstance;
    const userService = TestBed.inject(UserService);
    spyOn(userService, 'logout');
    component.logout();
    expect(userService.logout).toHaveBeenCalled();
  });
});