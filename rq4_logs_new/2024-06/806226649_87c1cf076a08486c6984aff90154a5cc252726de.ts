import {inject, Injectable} from "@angular/core";
import {HttpClient} from "@angular/common/http";
import {firstValueFrom, Observable, of, tap} from "rxjs";
import {Notification} from "../model/notification.model";
import {environment} from "../../../../environments/environment";
import {Toast, ToastService} from "../../../shared/components/toast/toast.service";
import {catchError} from "rxjs/operators";
import {mdiAlert, mdiCheck} from "@mdi/js";
import {ErrorResponseService} from "../../../shared/services/error-response.service";

@Injectable({
  providedIn: 'root'
})
export class NotificationService {

  private readonly httpService = inject(HttpClient);
  private readonly toastService = inject(ToastService);
  private readonly errorStatusService = inject(ErrorResponseService);

  getNotifications(id:number): Promise<Notification[]> {
    return firstValueFrom(this.httpService.get<Notification[]>(`${environment.baseUrl}/notifications/user/${id}`)
      .pipe(catchError(err => {
        const statusText = this.errorStatusService.getHttpErrorResponseTextByStatus(err.status);
        const errorToast: Toast = {classname: "bg-danger text-light", header: 'Could not load notifications',
          body: `${err.status} ${statusText}`, icon: mdiAlert, iconColor: "white"};
        this.toastService.show(errorToast);
        return of([]);
      })));
  }

  sendDeleteNotification(id:number): Promise<void> {
    return firstValueFrom(this.httpService.delete<void>(`${environment.baseUrl}/notifications/user/${id}`)
      .pipe(tap(() => {
          const successToast: Toast = {classname: "bg-success text-light", header: '',
            body: "Deleted all notifications successfully", icon: mdiCheck, iconColor: "white"};
          this.toastService.show(successToast);
        }),
        catchError(err => {
        const statusText = this.errorStatusService.getHttpErrorResponseTextByStatus(err.status);
        const errorToast: Toast = {classname: "bg-danger text-light", header: 'Could not delete notifications',
          body: `${err.status} ${statusText}`, icon: mdiAlert, iconColor: "white"};
        this.toastService.show(errorToast);
        return of();
      }))
    );
  }

}