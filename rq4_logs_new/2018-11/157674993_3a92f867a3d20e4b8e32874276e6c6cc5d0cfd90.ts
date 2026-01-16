import { Injectable } from '@angular/core';
import { Subject }    from 'rxjs';


@Injectable()
export class AnswerService {
  private kindSource = new Subject<string>();
  kind$ = this.kindSource.asObservable();
  private purpose$ = new Subject<string>();
  private when$ = new Subject<number>();
  private weeks$ = new Subject<number>();

  getKind() {
      return this.kind$;
  }
  updateKind(data: string) {
      this.kindSource.next(data);
  }

  getPurpose() {
        return this.purpose$;
  }
  updatePurpose(data: string) {
      this.purpose$.next(data);
  }

  getWhen() {
      return this.when$;
  }
  updateWhen(data: number) {
      this.when$.next(data);
  }
  getWeeks() {
      return this.weeks$;
  }
  updateWeeks(data: number) {
      this.weeks$.next(data);
  }
}