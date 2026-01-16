import { Inject, Directive, Input, ElementRef, OnInit } from '@angular/core';
import { I18NEXT_SERVICE, ITranslationService } from 'angular-i18next';

@Directive({
  selector: 'Trans'
})
export class I18NextFormatDirective implements OnInit {
  @Input('i18nKey') key: string;
  @Input() options: string;

  constructor(
    private el: ElementRef,
    @Inject(I18NEXT_SERVICE) private translateI18Next: ITranslationService
  ) {}

  ngOnInit() {
    const defaultText = this.el.nativeElement.innerHTML;
    const opts: any =
      typeof this.options === 'string'
        ? { format: this.options }
        : this.options;

    if (opts && opts.format) {
      this.el.nativeElement.innerHTML =
        this.translateI18Next.t(this.key, opts.format) || defaultText;
    } else {
      this.el.nativeElement.innerHTML =
        this.translateI18Next.t(this.key) || defaultText;
    }
  }
}