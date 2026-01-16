/**
 * Created by Samuel Gratzl on 24.11.2014.
 */
/// <amd-dependency path="bootstrap" />

/// <amd-dependency path="text!./header.html" />
declare var require:(moduleId:string) => any;
const template:string = require("text!./header.html");

import C = require('../caleydo_core/main');

export interface IHeaderLink {
  name: string;
  action: () => any;
  href: string
}

function createLi(name:string, action:() => any, href = '#') {
  const li = <HTMLElement>document.createElement('li');
  li.innerHTML = '<a href="' + href + '">' + name + '</a>';
  if (action) {
    (<HTMLElement>li.querySelector('a')).onclick = action;
  }
  return li;
}

export class AppHeader {
  private _options = {
    prepend : true,
    app: 'Caleydo Web',
    mainMenu: new Array<IHeaderLink>(),
    rightMenu: new Array<IHeaderLink>()
  };

  private mainMenu:HTMLElement;
  private rightMenu:HTMLElement;

  about:HTMLElement;
  options:HTMLElement;

  constructor(private parent:HTMLElement, options:any = {}) {
    C.mixin(this._options, options);

    //create the content and copy it in the parent
    const helper = document.createElement('div');
    helper.innerHTML = template;
    let old = parent.firstChild;
    if (this._options.prepend && old) {
      while (helper.firstChild) {
        parent.insertBefore(helper.firstChild, old);
      }
    } else {
      while (helper.firstChild) {
        parent.appendChild(helper.firstChild);
      }
    }

    //create handler
    (<HTMLElement>parent.querySelector('*[data-header="app"]')).innerHTML = this._options.app;

    this.mainMenu = <HTMLElement>parent.querySelector('*[data-header="main"]');
    this.rightMenu = <HTMLElement>parent.querySelector('*[data-header="rightmenu"]');
    this.about = <HTMLElement>parent.querySelector('*[data-header="about"]');
    this.options = <HTMLElement>parent.querySelector('*[data-header="options"]');

    this._options.mainMenu.forEach((l) => this.addMainMenu(l.name, l.action, l.href));
    this._options.rightMenu.forEach((l) => this.addRightMenu(l.name, l.action, l.href));
  }

  addMainMenu(name:string, action:() => any, href = '#') {
    const li = createLi(name, action, href);
    this.mainMenu.appendChild(li);
    return li;
  }

  insertCustomMenu(element: Element) {
    this.rightMenu.parentElement.insertBefore(element, this.rightMenu);
  }

  insertCustomRightMenu(element: Element) {
    this.rightMenu.parentElement.appendChild(element);
  }

  addRightMenu(name:string, action:() => any, href = '#') {
    const li = createLi(name, action, href);
    this.rightMenu.insertBefore(li, this.rightMenu.firstChild);
    return li;
  }
}

export function create(parent:HTMLElement, options:any = {}) {
  return new AppHeader(parent, options);
}