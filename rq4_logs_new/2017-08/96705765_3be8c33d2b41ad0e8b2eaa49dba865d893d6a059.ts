import {
  OnInit,
  OnDestroy,
  ContentChildren,
  Directive,
  HostBinding,
  HostListener,
  Input,
  QueryList, AfterContentInit
} from '@angular/core';
import {ExpandableService} from './expand.service';
import {Router, RouterLinkWithHref} from '@angular/router';


@Directive({
  selector: 'cp-menu-list[cpSubList], cp-menu-list[sub-list]'
})

export class SubListDirective {
  @HostBinding('class.sub-menu') private subList: boolean;

  constructor() {
    this.subList = true;
  }
}

@Directive({
  selector: 'cp-menu-item[cpHasSub], cp-menu-item[has-sub]'
})

export class HasSubDirective implements AfterContentInit {
  @HostBinding('class.has-sub') private _hasSub: boolean;
  @HostBinding('class.expand') isExpanded: boolean;
  @ContentChildren(RouterLinkWithHref, {descendants: true}) linksWithHrefs: QueryList<RouterLinkWithHref>;

  @Input('has-sub')
  get hasSub() {return this._hasSub}
  set hasSub(value: boolean) {
    this._hasSub = value;
  }

  public itemName: string;
  @Input()
  set name(value: string) {
    this.itemName = value;
  }

  public parentName: string;
  @Input()
  set parent(value: string) {
    this.parentName = value;
  }
  constructor(
    private expandable: ExpandableService,
    private router: Router,
  ) {
    this.isExpanded = false;
  }

  @HostListener('click')
  onClick(): boolean {
    if (this.isExpanded) {
      this.expandable.emitChange({});
    } else {
      this.expandable.emitChange(this);
    }
    return false;
  }

  // following methods are similar to methods of RouterLinkActive
  // since those methods are private and not able to access from outside,
  // I was able to make sidebar working correctly without
  // subscribing changes on RouterLinkWithHref
  isLinkActive(router: Router): (link: (RouterLinkWithHref)) => boolean {
    return (link: RouterLinkWithHref) =>
      router.isActive(link.urlTree, false);
  }

  hasActiveLinks(): boolean {
    return this.linksWithHrefs.some(this.isLinkActive(this.router));
  }

  update(): void {
    if (!this.linksWithHrefs || !this.router.navigated) {
      return;
    }
    const hasActiveLinks = this.hasActiveLinks();
    this.isExpanded = hasActiveLinks;
  }

  ngAfterContentInit() {
    this.linksWithHrefs.changes.subscribe(activeRoute => {
      this.update();
    });
    this.update();
  }
}



@Directive({
  selector: 'cp-menu[cpExpandable], cp-menu[expandable]',

})

export class ExpandableDirective implements OnInit, OnDestroy {
  @ContentChildren(HasSubDirective, {descendants: true}) nestedItems: QueryList<HasSubDirective>;

  private expandedList$;

  constructor(
    private expandable: ExpandableService
  ) {}

  ngOnInit() {
    this.expandedList$ = this.expandable.changeEmitted$
      .subscribe(expandedList => {
        this.nestedItems.map(item => {
          if (item.itemName === expandedList.itemName) {
            item.isExpanded = true;
          } else {
            if (item.isExpanded && item.parentName && expandedList.itemName === item.parentName) {
              item.isExpanded = true;
            } else {
              item.isExpanded = false;
            }
          }
        });
        return expandedList;
      });
  }

  ngOnDestroy() {
    if (this.expandedList$) {
      this.expandedList$.unsubscribe();
    }
  }
}