import { Directive, Input, OnInit, OnDestroy } from "@angular/core";
import { Router, NavigationEnd } from "@angular/router";
import { BreakpointObserver } from "@angular/cdk/layout";
import { MatSidenav } from "@angular/material/sidenav";
import { Subject } from "rxjs";
import { map, takeUntil, filter } from "rxjs/operators";

@Directive({
  selector: "[permanentAt]"
})
export class SidenavDirective implements OnInit, OnDestroy {
  @Input() canOpen = () => true;
  @Input() permanentAt: number | undefined;

  destroy$ = new Subject();

  constructor(
    private router: Router,
    private breakpoint: BreakpointObserver,
    private sidenav: MatSidenav
  ) {}

  ngOnInit() {
    const permanent$ = this.breakpoint
      .observe(`(min-width: ${this.permanentAt}px)`)
      .pipe(
        takeUntil(this.destroy$),
        map(({ matches }) => matches)
      );

    permanent$.subscribe(permanent => {
      this.sidenav.mode = permanent ? "side" : "over";
      this.sidenav.opened = permanent && this.canOpen();
    });

    this.router.events
      .pipe(
        takeUntil(this.destroy$),
        filter(() => this.sidenav.mode === "over"),
        filter(event => event instanceof NavigationEnd)
      )
      .subscribe(() => this.sidenav.close());
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}