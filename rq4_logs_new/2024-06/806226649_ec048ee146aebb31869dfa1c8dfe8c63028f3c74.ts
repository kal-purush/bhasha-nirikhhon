import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';

import { ChangeAccountComponent } from './change-account.component';
import { IconComponent } from '../../../../shared/components/icon/icon/icon.component';
import { AuthState } from '../../../../core/auth/+state/auth.state';
import { Router, RouterModule } from '@angular/router';
import { routes } from '../../../../app.routes';
import { AccountService } from '../../service/account.service';
import { ToastService } from '../../../../shared/components/toast/toast.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { By } from '@angular/platform-browser';
import { changedUserMock, userMock } from '../../../../../../mock/user-mock';
import { mdiCheck } from '@mdi/js';
import { NgZone } from '@angular/core';

describe('ChangeAccountComponent Test', () => {
  let component: ChangeAccountComponent;
  let fixture: ComponentFixture<ChangeAccountComponent>;
  let authState: AuthState;
  let accountService: AccountService;
  let toastService: ToastService;
  let router: Router;
  let ngZone: NgZone;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [],
      imports: [ChangeAccountComponent,
        IconComponent,
        HttpClientTestingModule,
        RouterModule.forRoot(routes)],
      providers: []
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(ChangeAccountComponent);
    authState = TestBed.inject(AuthState);
    accountService = TestBed.inject(AccountService);
    toastService = TestBed.inject(ToastService);
    router = TestBed.inject(Router);
    ngZone = TestBed.inject(NgZone);

    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should disable input forms onInit', () => {
    const debugElement = fixture.debugElement;
    const usernameInput = debugElement.query(By.css('#change-username-input')).nativeElement as HTMLInputElement;
    const emailInput = debugElement.query(By.css('#change-email-input')).nativeElement as HTMLInputElement;

    expect(usernameInput.disabled).toBeTruthy();
    expect(emailInput.disabled).toBeTruthy();
  });

  it('should click on "Change" button and enable input forms', fakeAsync(() => {
    const debugElement = fixture.debugElement;
    const changeButton = debugElement.query(By.css('#changeData-button')).nativeElement as HTMLButtonElement;
    const usernameInput = debugElement.query(By.css('#change-username-input')).nativeElement as HTMLInputElement;
    const emailInput = debugElement.query(By.css('#change-email-input')).nativeElement as HTMLInputElement;

    changeButton.click();
    fixture.detectChanges();

    tick(100)

    const saveButton = debugElement.query(By.css('#saveData-button')).nativeElement as HTMLButtonElement;

    expect(usernameInput.disabled).toBeFalsy();
    expect(emailInput.disabled).toBeFalsy();
    expect(saveButton).toBeTruthy();
  }));

  it('should handle saveData correctly', async () => {
    // given
    authState.setUser(userMock);
    component.ngOnInit(); // patch form
    jest.spyOn(accountService, 'saveUserData').mockResolvedValue(changedUserMock);
    jest.spyOn(authState, 'setUser');
    const expectedToast = {classname: "bg-success text-light", header: '',
      body: "Saved changes successfully", icon: mdiCheck, iconColor: "white"};
    jest.spyOn(toastService, 'show');
    jest.spyOn(component.accountForm, 'patchValue');
    component.accountForm.get('username')?.setValue('newUsername');

    // when
    await component.saveData();

    // then
    expect(accountService.saveUserData).toHaveBeenCalledWith({
      id: userMock.id,
      username: "newUsername",
      email: userMock.email
    });
    expect(authState.setUser).toHaveBeenCalledWith(changedUserMock);
    expect(toastService.show).toHaveBeenCalledWith(expectedToast);
    expect(component.accountForm.patchValue).toHaveBeenCalledWith(changedUserMock);
  });

  it('should navigate to change password section', async () => {
    // given
    jest.spyOn(router, 'navigate');

    // when
    await ngZone.run(async () => component.changePw());

    // then
    expect(router.navigate).toHaveBeenCalledWith(['account/change-pw']);
  });

  it('should return false for dataHasChanged', fakeAsync(() =>  {
    authState.setUser(userMock);
    component.ngOnInit();

    const debugElement = fixture.debugElement;
    const changeButton = debugElement.query(By.css('#changeData-button')).nativeElement as HTMLButtonElement;
    changeButton.click();

    fixture.detectChanges();
    tick(100);

    const saveButton = debugElement.query(By.css('#saveData-button')).nativeElement as HTMLButtonElement;
    expect(component.dataHasChanged()).toBeFalsy();
    expect(saveButton.disabled).toBeTruthy();
  }));

  it('should return true for dataHasChanged', fakeAsync(() =>  {
    authState.setUser(userMock);
    component.ngOnInit();

    const debugElement = fixture.debugElement;
    const changeButton = debugElement.query(By.css('#changeData-button')).nativeElement as HTMLButtonElement;
    const usernameInput = debugElement.query(By.css('#change-username-input')).nativeElement as HTMLInputElement;
    changeButton.click();

    usernameInput.value = 'newUsername';
    usernameInput.dispatchEvent(new Event('input'));
    fixture.detectChanges();
    tick(100);

    const saveButton = debugElement.query(By.css('#saveData-button')).nativeElement as HTMLButtonElement;
    expect(component.dataHasChanged()).toBeTruthy();
    expect(saveButton.disabled).toBeFalsy();
  }));

  it('should cancel data change', fakeAsync(() =>  {
    authState.setUser(userMock);
    component.ngOnInit();

    const debugElement = fixture.debugElement;
    const changeButton = debugElement.query(By.css('#changeData-button')).nativeElement as HTMLButtonElement;
    const usernameInput = debugElement.query(By.css('#change-username-input')).nativeElement as HTMLInputElement;
    const emailInput = debugElement.query(By.css('#change-email-input')).nativeElement as HTMLInputElement;
    changeButton.click();

    usernameInput.value = 'newUsername';
    usernameInput.dispatchEvent(new Event('input'));
    fixture.detectChanges();
    tick(100);

    const cancelButton = debugElement.query(By.css('#cancel-button')).nativeElement as HTMLButtonElement;
    cancelButton.click();
    fixture.detectChanges();
    tick(100);

    expect(usernameInput.value).toEqual(userMock.username);
    expect(usernameInput.disabled).toBeTruthy();
    expect(emailInput.disabled).toBeTruthy();
  }));

});