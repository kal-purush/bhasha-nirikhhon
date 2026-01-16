import {
  Component,
  Input,
  HostBinding,
  OnInit,
  ChangeDetectionStrategy,
} from '@angular/core';
import CodeHighlightService from '../../../services/code-highlight.service';
import './article-code.pcss';

export const ARTICLE_CODE_TYPE = 'code';

class CodeModel {
  codeType: string;
  code: string;
}

@Component({
  selector: 'ds-article-code',
  template: `
    <div class="ArticleCode__content">
      <h4 class="ArticleCode__code-type">{{codeType}}</h4>
      <pre class="ArticleCode__code">
        <code [attr.data-language]="content.codeType">{{content.code}}</code>
      </pre>
    </div>
  `,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export default class ArticleCode implements OnInit {
  @HostBinding('class.ArticleCode') rootClass: boolean = true;

  @Input() content: CodeModel;

  private CODE_TYPES: Array<object> = [
    { text: 'JavaScript', value: 'javascript' },
    { text: 'HTML', value: 'html' },
    { text: 'CSS', value: 'css' },
    { text: 'JSON', value: 'json' },
    { text: 'Command Line', value: 'shell' },
  ];
  public codeType: string = '';

  constructor(public codeHighlightService: CodeHighlightService) {}

  ngOnInit() {
    this.codeType = (<any>this.CODE_TYPES.find((type:any) => type.value === this.content.codeType)).text;
    this.codeHighlightService.renderCode();
  }
}