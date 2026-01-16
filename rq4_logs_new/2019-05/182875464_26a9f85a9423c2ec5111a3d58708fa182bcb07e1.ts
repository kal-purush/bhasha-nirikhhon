import
{
	HttpErrorResponse,
	HttpHandler,
	HttpInterceptor,
	HttpRequest
} from '@angular/common/http';
import { Injectable } from '@angular/core';
import 'rxjs/add/operator/do';
import { Observable } from 'rxjs';
import { NotificationService } from '@services/notification/notification.service';


@Injectable()
export class HttpErrorInterceptor implements HttpInterceptor
{
	private readonly _notifyService: NotificationService;

	public constructor(notifyService: NotificationService)
	{
		this._notifyService = notifyService;
	}

	public intercept(req: HttpRequest<any>, next: HttpHandler): Observable<any>
	{
		return next.handle(req).do(() => { }, error => 
		{
			if (error instanceof ErrorEvent)
			{
				return;
			}
			if (error instanceof HttpErrorResponse)
			{
				if (error.status == 401)
				{
					this._notifyService.notify('Your session was expired. Please, relogin.');
				}
				else
				{
					this._notifyService.notify('Unfortunately, our server is down :( Try later...');
				}
			}
		});
	}
}