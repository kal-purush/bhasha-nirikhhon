class KintoneCommentQuoter {
  private parentUl: HTMLUListElement;
  constructor(parentUl) {
    this.parentUl = parentUl;
  }

  render() {
    const element: HTMLAnchorElement = document.createElement('a');
    element.innerText = '引用';
    element.addEventListener('click', this.quote.bind(this));
    element.classList.add('ocean-ui-comments-commentbase-quote');

    const li: HTMLLIElement = document.createElement('li');
    li.appendChild(element);

    this.parentUl.appendChild(li);
  }

  quote(event: Event): void {
    let selectedText: string = window.getSelection().toString();
    if (!selectedText) {
      selectedText = this.wholeText();
    };

    const responseLink: HTMLElement = <HTMLElement>this.parentUl.querySelector('.ocean-ui-comments-commentbase-comment');
    responseLink.click();

    const quotingNode: HTMLDivElement = this.quotingNode(selectedText);
    requestAnimationFrame(() => window.getSelection().getRangeAt(0).insertNode(quotingNode));
  }

  quotingNode(text: string): HTMLDivElement {
    const fontTag: HTMLFontElement = document.createElement('font');
    fontTag.style.color = '#999999';
    fontTag.innerText = '> ' + text.split('\n').join('\n> ');

    const italicTag: HTMLElement = document.createElement('i');
    italicTag.appendChild(fontTag);

    const divTag = document.createElement('div');
    divTag.appendChild(italicTag);
    return divTag;
  }

  wholeText(): string {
    const commentBaseBody: HTMLElement = <HTMLElement>this.parentUl.closest('.ocean-ui-comments-commentbase-body');
    const commentBaseContents: HTMLElement = <HTMLElement>commentBaseBody.querySelector('.ocean-ui-comments-commentbase-contents');
    return commentBaseContents.innerText.trim();
  }
}

class KintoneCommentQuoterLooper {
  private modifiedDomIds: number[];

  constructor() {
    this.modifiedDomIds = [];
  }

  init() {
    this.loop();
  }

  static get DELAY() {
    return 300;
  }

  loop() {
    setInterval(this.step.bind(this), KintoneCommentQuoterLooper.DELAY);
  }

  step() {
    // ocean-ui-comments-comment-id-331590
    const commentBases = document.querySelectorAll('div.ocean-ui-comments-commentbase');
    Array.from(commentBases).forEach(commentBase => {
      const classString: string = commentBase.classList.toString();
      // ugly workaround but hey! Life is short
      const idContainedString: string | undefined = classString.split(' ').filter(s => /t-id-/.test(s)).pop();
      if (!idContainedString) return;
      const commentBaseId: number = Number(idContainedString.split('-').pop());

      if (this.modifiedDomIds.includes(commentBaseId)) return;
      this.modifiedDomIds.push(commentBaseId);
      const kintoneCommentQuoter = new KintoneCommentQuoter(commentBase.querySelector('.ocean-ui-comments-commentbase-actions'));
      kintoneCommentQuoter.render();
    })
  }
}

new KintoneCommentQuoterLooper().init();