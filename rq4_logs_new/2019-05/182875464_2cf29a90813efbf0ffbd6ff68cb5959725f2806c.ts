import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { UserService } from '@services/user/user.service';
import { Injectable } from '@angular/core';
import { NotificationService } from '@services/notification.service';

@Injectable()
export class UserOnlyGuard implements CanActivate
{
	private readonly _userService: UserService;
	private readonly _notifyService: NotificationService;

	public constructor(userService: UserService, notifyService: NotificationService)
	{
		this._userService = userService;
		this._notifyService = notifyService;
	}

	public canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean
	{
		if (!this._userService.loggedIn)
		{
			this._notifyService.notify('You should log in first');
			return false;
		}

		return true;
	}
}