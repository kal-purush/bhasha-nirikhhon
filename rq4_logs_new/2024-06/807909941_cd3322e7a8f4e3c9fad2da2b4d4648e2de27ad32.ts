import { UserService } from './user.service';
import { WithUserService, WaitForUser } from './auth.decorator';
import { ReplaySubject } from 'rxjs';

class TestClass implements WithUserService {
  isSignedIn$ = new ReplaySubject<boolean>(1);
  userService: jasmine.SpyObj<UserService> = jasmine.createSpyObj('UserService', [], { 'isSignedIn$': this.isSignedIn$ });

  @WaitForUser()
  async testMethod() {
    return true;
  }
}

describe('Auth Decorator', () => {
  beforeEach(() => {
    jasmine.clock().install();
  });
  afterEach(() => {
    jasmine.clock().uninstall();
  });

  it('waits for user', async () => {
    const testClass = new TestClass();
    const result = testClass.testMethod();
    testClass.isSignedIn$.next(true);
    await expectAsync(result).toBeResolvedTo(true);
  });

  it('rejects after timeout', async () => {
    const testClass = new TestClass();
    const result = testClass.testMethod();
    jasmine.clock().tick(2500);
    await expectAsync(result).toBeRejected();
  });
});