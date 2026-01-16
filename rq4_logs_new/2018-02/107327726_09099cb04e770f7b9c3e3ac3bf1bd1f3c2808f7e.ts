import { Directive, Input, OnDestroy, TemplateRef, ViewContainerRef } from '@angular/core';
import { ObservableMedia } from '@angular/flex-layout';
import { Subject } from 'rxjs/Subject';
import { combineLatest } from 'rxjs/observable/combineLatest';
import { startWith } from 'rxjs/operators';

// this is needed since fxHide does not actually remove hidden elements from the DOM
// this directive is the recommended work around for this issue
// see https://github.com/angular/flex-layout/issues/142 for more details
@Directive({
  selector: '[msMediaIf]',
})
export class MediaIfDirective implements OnDestroy {

  private hasView = false;
  private matcher = new Subject<string>();
  private subscription = combineLatest(
    this.matcher,
    this.media.asObservable().pipe(startWith(null)),
  )
    .subscribe(([matcher, _]) => {
      const condition = this.media.isActive(matcher);

      if (condition && !this.hasView) {
        this.viewContainer.createEmbeddedView(this.template);
        this.hasView = true;
      } else if (!condition && this.hasView) {
        this.viewContainer.clear();
        this.hasView = false;
      }
    });

  @Input()
  set msMediaIf(value: string) {
    this.matcher.next(value);
  }

  constructor(
    private template: TemplateRef<any>,
    private viewContainer: ViewContainerRef,
    private media: ObservableMedia,
  ) {}

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }
}